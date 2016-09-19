package eu.fbk.microneel.rewrite;

import java.io.IOException;
import java.nio.file.Path;

import com.google.gson.JsonObject;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.Annotation;
import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.microneel.util.CorpusStats;
import eu.fbk.microneel.util.Rewriting;

public final class AnnotationRewriter implements Annotator {

    private final CorpusStats corpus;

    public AnnotationRewriter(final JsonObject json, final Path path) throws IOException {
        final String[] relativePaths = json.get("corpus").getAsString().split("\\s+");
        final Path[] paths = new Path[relativePaths.length];
        for (int i = 0; i < relativePaths.length; ++i) {
            paths[i] = path.resolve(relativePaths[i]);
        }
        this.corpus = CorpusStats.forFiles(paths);
    }

    public AnnotationRewriter(final CorpusStats corpus) {
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

        // Replace URL, Mention, and Hashtag annotations. Skip other annotations
        for (final Annotation a : post.getAnnotations()) {

            // Replace URL -> "", hashtag -> hashtag/tokenization, mention -> fullname/username
            if (a instanceof UrlAnnotation) {
                rewriting.tryReplace(a.getBeginIndex(), a.getEndIndex(), "");
            } else if (a instanceof MentionAnnotation) {
                final MentionAnnotation ma = (MentionAnnotation) a;
                final String replacement = this.corpus.normalize(ma.getUsername(), true)
                        + (ma.getFullName() == null ? ""
                                : " / " + this.corpus.normalize(ma.getFullName(), true));
                // final String replacement = this.corpus.normalize(
                // ma.getFullName() != null ? ma.getFullName() : ma.getUsername(), true);
                rewriting.tryReplace(ma.getBeginIndex(), ma.getEndIndex(), replacement);
            } else if (a instanceof HashtagAnnotation) {
                final HashtagAnnotation ha = (HashtagAnnotation) a;
                final String replacement = a.getText().equalsIgnoreCase("#rt") ? ""
                        : this.corpus.normalize(ha.getTokenization() != null ? ha.getTokenization()
                                : ha.getHashtag(), true);
                rewriting.tryReplace(ha.getBeginIndex(), ha.getEndIndex(), replacement);
            } else {
                continue;
            }

            // Adjust text after the annotation
            if (a.getBeginIndex() > 0 || !(a instanceof UrlAnnotation)) {
                for (int i = a.getEndIndex(); i < text.length(); ++i) {
                    final char ch = text.charAt(i);
                    if (ch == '#' || ch == '@') {
                        rewriting.tryReplace(a.getEndIndex(), i, ", ");
                    } else if (Character.isUpperCase(ch)) {
                        rewriting.tryReplace(a.getEndIndex(), i, ". ");
                    } else if (!Character.isWhitespace(ch)) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
