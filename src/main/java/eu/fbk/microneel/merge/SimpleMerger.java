package eu.fbk.microneel.merge;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.EntityAnnotation;

public class SimpleMerger implements Annotator {

    public static final SimpleMerger INSTANCE = new SimpleMerger();

    private SimpleMerger() {
    }

    @Override
    public void annotate(final Post post) throws Throwable {

        // Simply map all entity annotations to other entity annotations with default qualifier,
        // ignoring overlap errors
        for (final EntityAnnotation sa : post.getAnnotations(EntityAnnotation.class, null)) {
            try {
                final EntityAnnotation ta = post.addAnnotation(EntityAnnotation.class,
                        sa.getBeginIndex(), sa.getEndIndex());
                if (ta.getCategory() == null) {
                    ta.setCategory(sa.getCategory());
                }
                if (ta.getUri() == null) {
                    ta.setUri(sa.getUri());
                }
            } catch (final Throwable ex) {
                // Ignore
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
