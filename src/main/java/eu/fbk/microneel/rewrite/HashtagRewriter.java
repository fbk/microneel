package eu.fbk.microneel.rewrite;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.util.Rewriting;

public final class HashtagRewriter implements Annotator {

    public static final HashtagRewriter INSTANCE = new HashtagRewriter();

    private HashtagRewriter() {
    }

    @Override
    public void annotate(final Post post) throws Throwable {

//        if (post.getText() =)
//        Rewriting rewriting = post.getRewriting();
//        if (rewriting == null) {
//            if (post.getText() == null) {
//                return;
//            }
//            rewriting = new Rewriting(post.getText());
//            post.setRewriting(rewriting);
//        }
//
//        for (final HashtagAnnotation a : post.getAnnotations(HashtagAnnotation.class)) {
//
//        }

        // TODO
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
