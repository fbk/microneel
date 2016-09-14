package eu.fbk.microneel.rewrite;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import edu.stanford.nlp.util.StringUtils;
import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.util.Rewriting;

public final class SlangRewriter implements Annotator {

    public static final SlangRewriter INSTANCE = new SlangRewriter();

    private static final Logger LOGGER = LoggerFactory.getLogger(SlangRewriter.class);

    private static final Map<String, String> REPLACEMENTS;

    static {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        try {
            for (final String line : Resources.readLines(
                    SlangRewriter.class.getResource("SlangRewriter.tsv"), Charsets.UTF_8)) {
                final String[] fields = line.split("\t");
                builder.put(fields[0], fields[1]);
            }
        } catch (final IOException ex) {
            throw new Error(ex);
        }
        REPLACEMENTS = builder.build();
    }

    @Override
    public void annotate(final Post post) throws Throwable {

        // Lookup existing rewriting or crate a new one
        final String text = post.getText();
        Rewriting rewriting = post.getRewriting();
        if (rewriting == null) {
            if (text == null) {
                return;
            }
            rewriting = new Rewriting(text);
            post.setRewriting(rewriting);
        }

        // Apply replacements
        int start = -1;
        for (int i = 0; i < text.length(); ++i) {
            if (Character.isLetterOrDigit(text.charAt(i))) {
                if (start < 0) {
                    start = i;
                }
            } else {
                if (start >= 0) {
                    final String word = text.substring(start, i);
                    String replacement = REPLACEMENTS.get(word.toLowerCase());
                    if (replacement != null) {
                        if (!word.toUpperCase().equals(word)
                                && Character.isUpperCase(word.charAt(0))) {
                            replacement = StringUtils.capitalize(replacement);
                        }
                        rewriting.tryReplace(start, i, replacement);
                        LOGGER.debug("Replaced {} with {}", word, replacement);
                    }
                    start = -1;
                }
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
