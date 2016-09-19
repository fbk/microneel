package eu.fbk.microneel.link;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post.MentionAnnotation;

public final class SmtSupervisedFeatureExtractor {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SmtSupervisedFeatureExtractor.class);

    private static final char[] SYMBOLS = new char[] { '"', '\'', ' ', '_' };

    private final HashMap<String, Integer> firstNames;

    private final HashMap<String, Integer> lastNames;

    private final HashMap<String, Integer> firstNamesExpanded;

    private final HashMap<String, Integer> lastNamesExpanded;

    private final Map<String, Map<String, Integer>> sets;

    private final Map<Map<String, Integer>, Integer> maxFrequencies;

    public SmtSupervisedFeatureExtractor(final int freqThreshold, final Path... paths)
            throws IOException {

        this.firstNames = new HashMap<>();
        this.lastNames = new HashMap<>();
        this.firstNamesExpanded = new HashMap<>();
        this.lastNamesExpanded = new HashMap<>();
        this.maxFrequencies = new HashMap<>();
        this.sets = new HashMap<>();
        this.sets.put("first", this.firstNames);
        this.sets.put("last", this.lastNames);
        this.sets.put("first_expanded", this.firstNamesExpanded);
        this.sets.put("last_expanded", this.lastNamesExpanded);

        for (final Path path : paths) {
            final CSVParser parser = new CSVParser(new FileReader(path.toFile()), CSVFormat.MYSQL);
            for (final CSVRecord record : parser) {
                addName(record.get(1), this.firstNames, this.firstNamesExpanded);
                addName(record.get(2), this.lastNames, this.lastNamesExpanded);
            }
            parser.close();
        }
        for (final Map.Entry<String, Map<String, Integer>> set : this.sets.entrySet()) {
            LOGGER.info("Filtering nameset: " + set.getKey());
            final Map<String, Integer> map = set.getValue();
            LOGGER.info("  Before filtering: " + map.size());
            final Iterator<Map.Entry<String, Integer>> entries = map.entrySet().iterator();
            int max = 0;
            while (entries.hasNext()) {
                final Map.Entry<String, Integer> entry = entries.next();
                if (max < entry.getValue()) {
                    max = entry.getValue();
                }
                if (entry.getValue() < freqThreshold) {
                    entries.remove();
                }
            }
            LOGGER.info("  After filtering: " + map.size());
            this.maxFrequencies.put(map, max);
        }
    }

    public Set<String> extractFeatures(final MentionAnnotation annotation) {

        // Fields features are extracted from
        final String username = annotation.getUsername();
        final String lang = annotation.getLang();
        final String uri = annotation.getUri();
        final Category uriCategory = annotation.getCategory();

        // Feature initialization
        final Set<String> features = Sets.newHashSet();
        final Collection<String> parts = breakUsername(username);

        // Log reporting
        final double random = new Random().nextDouble();
        if (random < 0.4) {
            LOGGER.info("Username: " + username + ", name: "
                    + (annotation.getFullName() == null ? "" : annotation.getFullName()));
            LOGGER.info("  Username parts: " + String.join(", ", parts));
            LOGGER.info("  Name parts: " + String.join(", ", parts));
        }

        // Filling array of features
        for (final String part : parts) {
            for (final Map.Entry<String, Map<String, Integer>> entry : this.sets.entrySet()) {
                final String setId = entry.getKey();
                final Map<String, Integer> set = entry.getValue();
                if (set.containsKey(part)) {
                    features.add("part_in_" + setId);
                }
            }
        }

        features.add(uri == null ? "uri_null" : "uri_exists");
        features.add(lang == null ? "lang_null" : "lang_" + lang.toLowerCase());
        features.add(uriCategory == null ? "uri_category_null"
                : "uri_category_" + uriCategory.toString().toLowerCase());

        for (final Map.Entry<String, Map<String, Integer>> entry : this.sets.entrySet()) {
            final String setId = entry.getKey();
            for (final String name : entry.getValue().keySet()) {
                if (username.toLowerCase().contains(name)) {
                    if (random < 0.4) {
                        LOGGER.info("  Matched[" + entry.getKey() + ", freq: "
                                + entry.getValue().get(name) + "]: " + name);
                    }
                    features.add("contains_" + setId);
                }
            }
        }

        // Combinations of features
        final List<String> uniqueFeatures = new ArrayList<>(features);
        for (final String feature1 : uniqueFeatures) {
            for (final String feature2 : uniqueFeatures) {
                if (feature1.compareTo(feature2) < 0) {
                    features.add(feature1 + "_AND_" + feature2);
                }
            }
        }
        return features;
    }

    private static void addName(String name, final HashMap<String, Integer> map1,
            final HashMap<String, Integer> map2) {
        name = name.toLowerCase().trim();
        if (name.length() < 3) {
            return;
        }
        map1.put(name, map1.getOrDefault(name, 0) + 1);
        for (final String part : parseName(name)) {
            map2.put(part, map2.getOrDefault(name, 0) + 1);
        }
    }

    private static List<String> parseName(final String name) {
        final List<String> result = new LinkedList<>();
        result.add(name);
        final String[] subnames = name.split(" ");
        for (String subname : subnames) {
            subname = trim(subname);
            if (subname.length() < 3) {
                continue;
            }
            result.add(subname);
        }
        return result;
    }

    private static String trim(final String string) {
        int beg = 0, end = string.length();
        while (beg < end && checkSymbol(string.charAt(beg))) {
            beg++;
        }
        while (beg < end && checkSymbol(string.charAt(end - 1))) {
            end--;
        }
        return beg > 0 && end < string.length() ? string.substring(beg, end) : string;
    }

    private static boolean checkSymbol(final char ch) {
        for (final char symbol : SYMBOLS) {
            if (ch == symbol) {
                return true;
            }
        }
        return false;
    }

    private static Collection<String> breakUsername(final String username) {
        final Set<String> result = new HashSet<>();
        for (final String part : username.split("[0-9]+|_")) {
            result.addAll(breakOnCamelCase(part));
        }
        return result;
    }

    private static LinkedList<String> breakOnCamelCase(final String username) {
        final LinkedList<String> parts = new LinkedList<>();
        if (username.length() == 0) {
            return parts;
        }
        parts.add(username.toLowerCase());

        int lastI = 0;
        for (int i = 1; i < username.length(); i++) {
            if (Character.isUpperCase(username.charAt(i))) {
                parts.add(username.substring(lastI, i).toLowerCase());
                lastI = i;
            }
        }
        if (lastI != 0) {
            parts.add(username.substring(lastI).toLowerCase());
        }
        return parts;
    }

}
