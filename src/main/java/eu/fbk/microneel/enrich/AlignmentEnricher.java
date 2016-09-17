package eu.fbk.microneel.enrich;

import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.net.UrlEscapers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.MentionAnnotation;

public final class AlignmentEnricher implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlignmentEnricher.class);

    private final String endpoint;

    public AlignmentEnricher(final JsonObject json) {
        this(json.has("endpoint") ? json.get("endpoint").getAsString() : null);
    }

    public AlignmentEnricher(@Nullable final String endpoint) {
        this.endpoint = endpoint == null ? "https://api.futuro.media/smt/alignments"
                : endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    }

    @Override
    public void annotate(final Iterable<Post> posts) throws Throwable {

        // Collect the usernames of the users for which we may fetch alignments
        final Set<String> usernames = new HashSet<>();
        for (final Post post : posts) {
            if (post.getAuthorUsername() != null
                    && (post.getAuthorUri() == null || post.getAuthorCategory() == null)) {
                usernames.add(post.getAuthorUsername().toLowerCase());
            }
            for (final MentionAnnotation m : post.getAnnotations(MentionAnnotation.class)) {
                if (m.getUri() == null || m.getCategory() == null) {
                    usernames.add(m.getUsername().toLowerCase());
                }
            }
        }

        // Fetch alignments
        LOGGER.debug("Looking up alignments for {} usernames", usernames.size());
        final Gson gson = new GsonBuilder().create();
        final Map<String, String> alignments = new HashMap<>();
        final Map<String, Category> categories = new HashMap<>();
        for (final String username : usernames) {
            try {
                final URL url = new URL(this.endpoint + "/by_twitter_username?username="
                        + UrlEscapers.urlFormParameterEscaper().escape(username));
                final String content = Resources.toString(url, Charsets.UTF_8);
                final JsonObject json = gson.fromJson(content, JsonObject.class);
                final JsonElement data = json.get("data");
                if (data instanceof JsonObject) {
                    final JsonElement alignment = ((JsonObject) data).get("alignment");
                    final JsonElement type = ((JsonObject) data).get("type");
                    if (type instanceof JsonPrimitive) {
                        final String typeStr = type.getAsString();
                        if (typeStr.equals("http://dbpedia.org/ontology/Person")) {
                            categories.put(username, Category.PERSON);
                        } else if (typeStr.equals("http://dbpedia.org/ontology/Organisation")
                                || typeStr.equals("http://dbpedia.org/ontology/Company")) {
                            categories.put(username, Category.ORGANIZATION);
                        }
                        LOGGER.debug("Category for {}: {}", username, categories.get(username));
                    }
                    if (alignment instanceof JsonPrimitive) {
                        alignments.put(username, alignment.getAsString());
                        LOGGER.debug("Alignment for {}: {}", username, alignment.getAsString());
                    }
                }
            } catch (final Throwable ex) {
                LOGGER.error("Failed to fetch alignment for username " + username, ex);
            }
        }

        // Enrich posts with found alignments
        for (final Post post : posts) {
            if (post.getAuthorUsername() != null) {
                final String key = post.getAuthorUsername().toLowerCase();
                if (post.getAuthorCategory() == null) {
                    post.setAuthorCategory(categories.get(key));
                }
                if (post.getAuthorUri() == null) {
                    post.setAuthorUri(alignments.get(key));
                }
            }
            for (final MentionAnnotation m : post.getAnnotations(MentionAnnotation.class)) {
                final String key = m.getUsername().toLowerCase();
                if (m.getCategory() == null) {
                    m.setCategory(categories.get(key));
                }
                if (m.getUri() == null) {
                    m.setUri(alignments.get(key));
                }
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.endpoint + ")";
    }

}
