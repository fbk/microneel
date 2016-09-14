package eu.fbk.microneel.rewrite;

import org.apache.commons.lang3.text.WordUtils;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.Annotation;
import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.microneel.util.Rewriting;

public final class AnnotationRewriter implements Annotator {

    public static final AnnotationRewriter INSTANCE = new AnnotationRewriter();

    private AnnotationRewriter() {
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
                final String replacement = WordUtils.capitalize(
                        normalize(ma.getFullName() != null ? ma.getFullName() : ma.getUsername()));
                rewriting.tryReplace(ma.getBeginIndex(), ma.getEndIndex(), replacement);
            } else if (a instanceof HashtagAnnotation) {
                final HashtagAnnotation ha = (HashtagAnnotation) a;
                final String replacement = normalize(
                        ha.getTokenization() != null ? ha.getTokenization() : ha.getHashtag());
                rewriting.tryReplace(ha.getBeginIndex(), ha.getEndIndex(), replacement);
            } else {
                continue;
            }

            // Adjust text after the annotation
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

    private static String normalize(final String string) {
        final StringBuilder builder = new StringBuilder(string);
        for (int i = 0; i < builder.length(); ++i) {
            final char c = builder.charAt(i);
            if (!Character.isLetterOrDigit(c)) {
                builder.setCharAt(i, ' ');
            } else {
                break;
            }
        }
        for (int i = string.length() - 1; i >= 0; --i) {
            final char c = builder.charAt(i);
            if (!Character.isLetterOrDigit(c)) {
                builder.setCharAt(i, ' ');
            } else {
                break;
            }
        }
        return builder.toString().trim();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
