package eu.fbk.microneel.rewrite;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;

public class MentionRewriter implements Annotator {

    public static final MentionRewriter INSTANCE = new MentionRewriter();

    private MentionRewriter() {
    }

    @Override
    public void annotate(final Post post) throws Throwable {
        // TODO
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
