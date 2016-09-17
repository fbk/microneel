package eu.fbk.microneel.link;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.EntityAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;

public final class SmtLinker implements Annotator {

    public static final SmtLinker INSTANCE = new SmtLinker();

    private SmtLinker() {
    }

    @Override
    public void annotate(final Post post) throws Throwable {

        // Generate an EntityAnnotation for each MentionAnnotation in the Post
        for (final MentionAnnotation ma : post.getAnnotations(MentionAnnotation.class)) {
            if (ma.getCategory() != null) {
                final EntityAnnotation a = post.addAnnotation(EntityAnnotation.class,
                        ma.getBeginIndex() + 1, ma.getEndIndex(), "smt");
                a.setCategory(ma.getCategory());
                a.setUri(ma.getUri());
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
