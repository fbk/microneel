package eu.fbk.microneel.enrich;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.HashtagAnnotation;

public final class TagdefEnricher implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TagdefEnricher.class);

    @Nullable
    private final String lang;

    public TagdefEnricher(final JsonObject json) {
        this(json.has("lang") ? json.get("lang").getAsString() : null);
    }

    public TagdefEnricher(@Nullable final String lang) {
        this.lang = lang == null ? null : Strings.emptyToNull(lang.trim());
    }

    @Override
    public void annotate(final Iterable<Post> posts) throws Throwable {

        // Collect the hashtags
        final Set<String> hashtags = new HashSet<>();
        for (final Post post : posts) {
            for (final HashtagAnnotation h : post.getAnnotations(HashtagAnnotation.class)) {
                hashtags.add(h.getHashtag().toLowerCase());
            }
        }

        // Call tagdef API
        LOGGER.debug("Fetching definitions for {} hashtags", hashtags.size());
        final Multimap<String, String> definitions = ArrayListMultimap.create();
        final Gson gson = new GsonBuilder().create();
        for (final String hashtag : hashtags) {
            boolean found = false;
            try {
                final URL url = new URL("https://api.tagdef.com/" + hashtag + ".json?no404=1"
                        + (this.lang == null ? "" : "&lang=" + this.lang.toLowerCase()));
                final String response = Resources.toString(url, Charsets.UTF_8);
                final JsonObject json = gson.fromJson(response, JsonObject.class);
                final JsonElement defsElement = json.get("defs");
                final Iterable<JsonElement> elements = defsElement instanceof JsonArray
                        ? (JsonArray) defsElement : ImmutableList.of(defsElement);
                for (final JsonElement element : elements) {
                    final JsonObject def = ((JsonObject) element).getAsJsonObject("def");
                    final JsonElement text = def.get("text");
                    if (text != null) {
                        found = true;
                        definitions.put(hashtag, text.getAsString());
                        LOGGER.debug("Definition found for #{}: {}", hashtag, def);
                    }
                }
            } catch (final FileNotFoundException ex) {
                // ignore
            } catch (final Throwable ex) {
                LOGGER.warn("Ignoring exception while fetching definitions for #" + hashtag, ex);
            }
            if (!found) {
                LOGGER.debug("No definition found for #{}", hashtag);
            }
            Thread.sleep(500); // Wait
        }

        // Enrich posts
        for (final Post post : posts) {
            for (final HashtagAnnotation a : post.getAnnotations(HashtagAnnotation.class)) {
                final Collection<String> defs = definitions.asMap()
                        .get(a.getHashtag().toLowerCase());
                if (defs != null && !defs.isEmpty()) {
                    final List<String> newDefs = new ArrayList<>();
                    if (a.getDefinitions() != null) {
                        newDefs.addAll(a.getDefinitions());
                    }
                    newDefs.addAll(defs);
                    a.setDefinitions(newDefs);
                }
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (this.lang == null ? "" : "(" + this.lang + ")");
    }

}
