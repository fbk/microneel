package eu.fbk.microneel.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.fbk.utils.core.IO;

public class CorpusStats {

    private static final Logger LOGGER = LoggerFactory.getLogger(CorpusStats.class);

    private static final Cache<Set<Path>, CorpusStats> CORPORA = CacheBuilder.newBuilder()
            .softValues().build();

    private final Map<String, Entry> entries;

    private final int totalOccurrences;

    private CorpusStats(final Map<String, Entry> entries, final int totalOccurrences) {
        this.entries = entries;
        this.totalOccurrences = totalOccurrences;
    }

    private static String wordFor(final String capitalization) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < capitalization.length(); ++i) {
            char ch = Character.toLowerCase(capitalization.charAt(i));
            if (ch == 'è' || ch == 'é') {
                ch = 'e';
            } else if (ch == 'à') {
                ch = 'a';
            } else if (ch == 'ì') {
                ch = 'i';
            } else if (ch == 'ò') {
                ch = 'o';
            } else if (ch == 'ù') {
                ch = 'ù';
            }
            builder.append(ch);
        }
        return builder.toString();
    }

    public static CorpusStats forFiles(final Path... paths) throws IOException {
        synchronized (CORPORA) {
            final Path[] normalizedPaths = paths.clone();
            for (int i = 0; i < paths.length; ++i) {
                normalizedPaths[i] = normalizedPaths[i].toAbsolutePath();
            }
            final Set<Path> key = ImmutableSet.copyOf(normalizedPaths);
            CorpusStats corpus = CORPORA.getIfPresent(key);
            if (corpus == null) {
                final Map<String, Entry> entries = new HashMap<>();
                int totalOccurrences = 0;
                for (final Path path : paths) {
                    LOGGER.info("Loading {}", path);
                    try (BufferedReader reader = new BufferedReader(
                            IO.utf8Reader(IO.read(path.toString())))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.isEmpty() || line.startsWith("#")) {
                                continue;
                            }
                            final String[] fields = line.split("\t");
                            final int occurrences = Integer.parseInt(fields[0]);
                            final String capitalization = fields[1].trim();
                            final String word = wordFor(capitalization);
                            Entry entry = entries.get(word);
                            if (entry == null) {
                                entry = new Entry(capitalization, occurrences);
                                entries.put(word, entry);
                            } else {
                                entry.add(capitalization, occurrences);
                            }
                            totalOccurrences += occurrences;
                        }
                    }
                }
                corpus = new CorpusStats(ImmutableMap.copyOf(entries), totalOccurrences);
                CORPORA.put(key, corpus);
            }
            return corpus;
        }
    }

    public int getTotalOccurrences() {
        return this.totalOccurrences;
    }

    public int getWordOccurrences(final String word) {
        final Entry entry = this.entries.get(wordFor(word));
        return entry == null ? 0 : entry.getOccurrences();
    }

    public int getCapitalizationOccurrences(final String capitalization) {
        final Entry entry = this.entries.get(wordFor(capitalization));
        return entry == null ? 0 : entry.getOccurrences(capitalization);
    }

    public Set<String> getCapitalizationsOf(final String word) {
        final Entry entry = this.entries.get(wordFor(word));
        return entry == null ? ImmutableSet.of() : entry.getCapitalizations();
    }

    @Nullable
    public String getMostFrequentCapitalization(final String word, final float ratio) {
        final Entry entry = this.entries.get(wordFor(word));
        return entry == null ? null : entry.getMostFrequentCapitalization(ratio);
    }

    public String normalize(final String string, final boolean retokenize) {

        final StringBuilder builder = new StringBuilder(string.length() * 2);
        for (int i = 0; i < string.length(); ++i) {
            final char c = string.charAt(i);
            builder.append(
                    Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '\'' ? c : ' ');
        }
        final String cleanString = builder.toString();

        builder.setLength(0);
        for (final String token : cleanString.split("\\s+")) {
            normalizeHelper(builder, token, retokenize);
        }
        final String normalizedString = builder.toString();

        LOGGER.debug("Normalized {} -> {}", string, normalizedString);
        return normalizedString;
    }

    private void normalizeHelper(final StringBuilder builder, String token,
            final boolean retokenize) {

        final Set<String> capitalizations = getCapitalizationsOf(token);
        if (capitalizations.contains(token)) {
            builder.append(builder.length() == 0 ? "" : " ").append(token);
            return;
        } else if (capitalizations.size() == 1) {
            final String capitalization = capitalizations.iterator().next();
            builder.append(builder.length() == 0 ? "" : " ").append(capitalization);
            if (!capitalization.equals(token)) {
                LOGGER.debug("Replacing {} with unique capitalization {}", token, capitalization);
            }
            return;
        } else if (!capitalizations.isEmpty()) {
            if (Character.isUpperCase(token.charAt(0)) && !token.toUpperCase().equals(token)) {
                final String capitalizedForm = StringUtils.capitalize(token.toLowerCase());
                if (capitalizations.contains(capitalizedForm)) {
                    LOGGER.debug("Replacing {} with capitalized form {}", token, capitalizedForm);
                    token = capitalizedForm;
                }
            } else {
                final String lowercaseForm = token.toLowerCase();
                if (capitalizations.contains(lowercaseForm)) {
                    LOGGER.debug("Replacing {} with lowercase form {}", token, lowercaseForm);
                    token = lowercaseForm;
                }
            }
            builder.append(builder.length() == 0 ? "" : " ").append(token);
            return;
        }

        int start = 0;
        if (retokenize) {
            for (int i = 0; i < token.length(); ++i) {
                final char c2 = token.charAt(i);
                if (i > 0 && Character.isLetterOrDigit(c2)) {
                    final char c1 = token.charAt(i - 1);
                    if (Character.isLetterOrDigit(c1)
                            && (Character.isUpperCase(c2) && !Character.isUpperCase(c1)
                                    || Character.isDigit(c2) && !Character.isDigit(c1)
                                    || !Character.isDigit(c2) && Character.isDigit(c2))) {
                        normalizeHelper(builder, token.substring(start, i), false);
                        start = i;
                    }
                }
            }
        }

        if (start > 0) {
            normalizeHelper(builder, token.substring(start), false);
        } else {
            builder.append(builder.length() == 0 ? "" : " ").append(token);
        }
    }

    private static final class Entry {

        private String[] capitalizations;

        private int[] occurrences;

        Entry(final String capitalization, final int occurrences) {
            this.capitalizations = new String[] { capitalization };
            this.occurrences = new int[] { occurrences };
        }

        void add(final String capitalization, final int occurrences) {
            for (int i = 0; i < this.capitalizations.length; ++i) {
                if (this.capitalizations[i].equals(capitalization)) {
                    this.occurrences[i] += occurrences;
                    return;
                }
            }
            final int newLength = this.capitalizations.length + 1;
            this.capitalizations = Arrays.copyOf(this.capitalizations, newLength);
            this.occurrences = Arrays.copyOf(this.occurrences, newLength);
            this.capitalizations[newLength - 1] = capitalization;
            this.occurrences[newLength - 1] = occurrences;
        }

        String getMostFrequentCapitalization(final float ratio) {
            int occurrences = -1;
            int occurrencesSecond = 1;
            String result = null;
            for (int i = 0; i < this.capitalizations.length; ++i) {
                if (this.occurrences[i] > occurrences || this.occurrences[i] == occurrences
                        && this.capitalizations[i].equals(this.capitalizations[i].toLowerCase())) {
                    occurrencesSecond = Math.max(1, occurrences);
                    occurrences = this.occurrences[i];
                    result = this.capitalizations[i];
                }
            }
            if ((float) occurrences / occurrencesSecond < ratio) {
                return null;
            }
            return result;
        }

        Set<String> getCapitalizations() {
            return ImmutableSet.copyOf(this.capitalizations);
        }

        Set<String> getCapitalizations(final int minOccurrences) {
            final List<String> result = new ArrayList<>();
            for (int i = 0; i < this.capitalizations.length; ++i) {
                if (this.occurrences[i] >= minOccurrences) {
                    result.add(this.capitalizations[i]);
                }
            }
            return ImmutableSet.copyOf(result);
        }

        int getOccurrences(final String capitalization) {
            for (int i = 0; i < this.capitalizations.length; ++i) {
                if (this.capitalizations[i].equals(capitalization)) {
                    return this.occurrences[i];
                }
            }
            return 0;
        }

        int getOccurrences() {
            int total = 0;
            for (final int occurrence : this.occurrences) {
                total += occurrence;
            }
            return total;
        }

    }

}
