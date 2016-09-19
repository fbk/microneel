package eu.fbk.microneel.rewrite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.util.CorpusStats;
import eu.fbk.microneel.util.Rewriting;

public class CleaningRewriter implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleaningRewriter.class);

    private static final Set<String> APOSTROPHE_WORDS = ImmutableSet.of("fa", "va", "sta", "da",
            "po", "mo");

    private static final Set<String> EMOTICONS = ImmutableSet.of(":‑)", ":)", ":-]", ":]", ":-3",
            ":3", ":->", ":>", "8-)", "8)", ":-}", ":}", ":o)", ":c)", ":^)", "=]", "=)", ":‑D",
            ":D", "8‑D", "8D", "x‑D", "xD", "X‑D", "XD", "=D", "=3", "B^D", ":))", ":-))", ":‑(",
            ":(", ":‑c", ":c", ":‑<", ":<", ":‑[", ":[", ":-||", ">:[", ":{", ":@", ">:(", ":'‑(",
            ":'(", ":'‑)", ":')", "D‑':", "D:<", "D:", "D8", "D;", "D=", "DX", ":‑O", ":O", ":‑o",
            ":o", ":-0", "8‑0", ">:O", ":-*", ":*", ":×", ";‑)", ";)", "*-)", "*)", ";‑]", ";]",
            ";^)", ":‑,", ";D", ":‑P", ":P", "X‑P", "XP", "x‑p", "xp", ":‑p", ":p", ":‑Þ", ":Þ",
            ":‑þ", ":þ", ":‑b", ":b", "d:", "=p", ">:P", ":‑/", ":/", ":‑.", ">:\\", ">:/", ":\\",
            "=/", "=\\", ":L", "=L", ":S", ":‑|", ":|", ":$", ":‑X", ":X", ":‑#", ":#", ":‑&", ":&",
            "O:‑)", "O:)", "0:‑3", "0:3", "0:‑)", "0:)", "0;^)", ">:‑)", ">:)", "}:‑)", "}:)",
            "3:‑)", "3:)", ">;)", "|;‑)", "|‑O", ":‑J", "#‑)", "%‑)", "%)", ":‑###..", ":###..",
            "<:‑|", "~(_8^(I)", "5:‑)", "~:‑\\ ", "*<|:‑)", "=:o]", "7:^]", ",:‑)", "</3", "<\3",
            "<3", "@};-", "@}->--", "@}‑;‑'‑‑‑", "@>‑‑>‑‑", "><>", "<*)))‑{", "><(((*>", "\\o/",
            "*\0/*", "//0‑0\\", "v.v", "O_O", "o‑o", "O_o", "o_O", "o_o", "O-O", ">.<", "^5",
            "o/\\o", ">_>^", "^<_<");

    private final CorpusStats corpus;

    public CleaningRewriter(final JsonObject json, final Path path) throws IOException {
        final String[] relativePaths = json.get("corpus").getAsString().split("\\s+");
        final Path[] paths = new Path[relativePaths.length];
        for (int i = 0; i < relativePaths.length; ++i) {
            paths[i] = path.resolve(relativePaths[i]);
        }
        this.corpus = CorpusStats.forFiles(paths);
    }

    public CleaningRewriter(final CorpusStats corpus) {
        this.corpus = corpus;
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

        // Replace emoticons
        replaceEmoticons(rewriting);

        // Replace a', e', i', o', u' with à, è, ì, ò, ù.
        replaceApostrophes(rewriting);

        // Fix capitalization
        replaceCapitalization(rewriting);

        // Replace escaped quotes and some html entities
        rewriting.tryReplace("\\\"", "\"", false);
        rewriting.tryReplace("&lt;", "<", false);
        rewriting.tryReplace("&gt;", ">", false);
        rewriting.tryReplace("&amp;", "&", false);
    }

    private void replaceEmoticons(final Rewriting rewriting) {
        final String text = rewriting.getOriginalString();
        int start = -1;
        for (int i = 0; i <= text.length(); ++i) {
            if (i < text.length() && !Character.isWhitespace(text.charAt(i))) {
                if (start < 0) {
                    start = i;
                }
            } else {
                if (start >= 0) {
                    final String token = text.substring(start, i);
                    if (EMOTICONS.contains(token)) {
                        rewriting.tryReplace(start, i, "");
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Removed emoticon {} from \"{}\"", token, text.substring(
                                    Math.max(0, start - 10), Math.min(text.length(), i + 10)));
                        }
                    }
                    start = -1;
                }
            }
        }
    }

    private void replaceApostrophes(final Rewriting rewriting) {
        final String text = rewriting.getOriginalString().toLowerCase();
        boolean maybeQuote = false;
        for (int index = text.indexOf('\''); index >= 0; index = text.indexOf('\'', index + 1)) {
            int start = index;
            while (start > 0 && Character.isLetterOrDigit(text.charAt(start - 1))) {
                --start;
            }
            final String word = text.substring(start, index);
            final boolean afterLetter = !word.isEmpty();
            final boolean beforeLetter = index < text.length() - 1
                    && Character.isLetter(text.charAt(index + 1));
            if (!afterLetter && beforeLetter) {
                maybeQuote = true;
            } else if (maybeQuote && afterLetter && !beforeLetter) {
                maybeQuote = false;
            } else if (!maybeQuote && afterLetter && !beforeLetter
                    && !APOSTROPHE_WORDS.contains(word)) {
                final char ch = text.charAt(index - 1);
                if (ch == 'a') {
                    rewriting.tryReplace(index - 1, index + 1, "à");
                } else if (ch == 'e') {
                    rewriting.tryReplace(index - 1, index + 1, "è");
                } else if (ch == 'i') {
                    rewriting.tryReplace(index - 1, index + 1, "ì");
                } else if (ch == 'o') {
                    rewriting.tryReplace(index - 1, index + 1, "ò");
                } else if (ch == 'u') {
                    rewriting.tryReplace(index - 1, index + 1, "ù");
                }
            }
        }
    }

    private void replaceCapitalization(final Rewriting rewriting) {
        final String text = rewriting.getOriginalString();
        int start = -1;
        for (int i = 0; i <= text.length(); ++i) {
            if (i < text.length() && Character.isLetterOrDigit(text.charAt(i))) {
                if (start < 0) {
                    start = i;
                }
            } else {
                if (start >= 0) {
                    final String token = text.substring(start, i);
                    final String normalizedToken = this.corpus.normalize(token, false);
                    if (!normalizedToken.equals(token)) {
                        rewriting.tryReplace(start, i, normalizedToken);
                        LOGGER.debug("Normalized {} -> {}", token, normalizedToken);
                    }
                    start = -1;
                }
            }
        }
    }

}
