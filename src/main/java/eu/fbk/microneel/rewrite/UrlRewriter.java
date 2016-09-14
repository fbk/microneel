package eu.fbk.microneel.rewrite;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;

public class UrlRewriter implements Annotator {

    public static final UrlRewriter INSTANCE = new UrlRewriter();

    private UrlRewriter() {
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
