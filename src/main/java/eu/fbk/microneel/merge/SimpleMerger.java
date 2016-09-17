package eu.fbk.microneel.merge;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.EntityAnnotation;

public class SimpleMerger implements Annotator {

    private final String[] qualifiers;

    public SimpleMerger(final JsonObject json) {
        if (!json.has("q")) {
            this.qualifiers = new String[] {};
        } else {
            final JsonArray array = json.get("q").getAsJsonArray();
            this.qualifiers = new String[array.size()];
            for (int i = 0; i < array.size(); ++i) {
                this.qualifiers[i] = array.get(i).getAsString();
            }
        }
    }

    public SimpleMerger(final String... qualifiers) {
        this.qualifiers = qualifiers.clone();
    }

    @Override
    public void annotate(final Post post) throws Throwable {

        // Simply map all entity annotations to other entity annotations with default qualifier,
        // ignoring overlap errors
        for (final String qualifier : this.qualifiers) {
            for (final EntityAnnotation sa : post.getAnnotations(EntityAnnotation.class,
                    qualifier)) {
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
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
