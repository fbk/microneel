package eu.fbk.microneel;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.fbk.utils.core.IO;

public final class Post implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;

    @Nullable
    private Date date;

    @Nullable
    private String text;

    @Nullable
    private String authorUsername;

    @Nullable
    private String authorFullName;

    @Nullable
    private String authorDescription;

    @Nullable
    private String authorUri;

    private final List<Annotation> annotations;

    @Nullable
    private Rewriting rewriting;

    @Nullable
    private String context;

    public Post(final JsonObject json) {
        this.id = json.get("id").getAsString();
        this.date = json.has("date") ? new Date(json.get("date").getAsLong() * 1000) : null;
        this.text = json.has("text") ? json.get("text").getAsString() : null;
        final JsonObject author = json.getAsJsonObject("author");
        this.authorUsername = author.has("username") ? author.get("username").getAsString() : null;
        this.authorFullName = author.has("fullName") ? author.get("fullName").getAsString() : null;
        this.authorDescription = author.has("description") ? author.get("description").getAsString()
                : null;
        this.authorUri = author.has("uri") ? author.get("uri").getAsString() : null;
        final JsonArray array = json.getAsJsonArray("annotations");
        this.annotations = new ArrayList<>();
        for (final JsonElement element : array) {
            final JsonObject e = (JsonObject) element;
            if (e.has("username")) {
                this.annotations.add(new MentionAnnotation(e));
            } else if (e.has("hashtag")) {
                this.annotations.add(new HashtagAnnotation(e));
            } else if (e.has("url")) {
                this.annotations.add(new UrlAnnotation(e));
            } else {
                throw new IllegalArgumentException("Unknown annotation: " + e);
            }
        }
        Collections.sort(this.annotations);
        if (json.has("rewriting")) {
            final JsonObject r = json.get("rewriting").getAsJsonObject();
            final JsonObject rc = new JsonObject();
            for (final Entry<String, JsonElement> e : r.entrySet()) {
                rc.add(e.getKey(), e.getValue());
            }
            rc.addProperty("from", this.text);
            this.rewriting = new Rewriting(rc);
        }
        this.context = json.has("context") ? json.get("context").getAsString() : null;
    }

    public Post(final String id) {
        this.id = Objects.requireNonNull(id);
        this.date = null;
        this.text = null;
        this.authorUsername = null;
        this.authorFullName = null;
        this.authorDescription = null;
        this.authorUri = null;
        this.annotations = new ArrayList<>();
    }

    public String getId() {
        return this.id;
    }

    @Nullable
    public Long getTwitterId() {
        return this.id.startsWith("twitter:") ? Long.valueOf(this.id.substring(8)) : null;
    }

    @Nullable
    public Date getDate() {
        return this.date;
    }

    public void setDate(@Nullable final Date date) {
        this.date = date;
    }

    @Nullable
    public String getText() {
        return this.text;
    }

    public void setText(@Nullable final String text) {
        if (!Objects.equals(this.text, text)) {
            if (text == null) {
                this.annotations.clear();
            } else {
                for (final Iterator<Annotation> i = this.annotations.iterator(); i.hasNext();) {
                    final Annotation annotation = i.next();
                    if (annotation.getEndIndex() > text.length() || !annotation.getText().equals(
                            text.substring(annotation.getBeginIndex(), annotation.getEndIndex()))) {
                        i.remove();
                    }
                }
            }
            this.text = text;
        }
    }

    @Nullable
    public String getAuthorUsername() {
        return this.authorUsername;
    }

    public void setAuthorUsername(@Nullable final String authorUsername) {
        this.authorUsername = authorUsername;
    }

    @Nullable
    public String getAuthorFullName() {
        return this.authorFullName;
    }

    @Nullable
    public void setAuthorFullName(@Nullable final String authorFullName) {
        this.authorFullName = authorFullName;
    }

    @Nullable
    public String getAuthorDescription() {
        return this.authorDescription;
    }

    public void setAuthorDescription(@Nullable final String authorDescription) {
        this.authorDescription = authorDescription;
    }

    @Nullable
    public String getAuthorUri() {
        return this.authorUri;
    }

    public void setAuthorUri(@Nullable final String authorUri) {
        this.authorUri = authorUri;
    }

    public List<Annotation> getAnnotations() {
        return ImmutableList.copyOf(this.annotations);
    }

    public <T extends Annotation> List<T> getAnnotations(final Class<T> annotationClazz) {
        Objects.requireNonNull(annotationClazz);
        final List<T> result = new ArrayList<>();
        for (final Annotation annotation : this.annotations) {
            if (annotationClazz.isInstance(annotation)) {
                result.add(annotationClazz.cast(annotation));
            }
        }
        return result;
    }

    public List<Annotation> getAnnotations(final int index) {
        final List<Annotation> result = new ArrayList<>();
        for (final Annotation annotation : this.annotations) {
            if (index >= annotation.getBeginIndex() && index < annotation.getEndIndex()) {
                result.add(annotation);
            }
        }
        return result;
    }

    public <T extends Annotation> T getAnnotation(final int index, final Class<T> annotationClazz) {
        for (final Annotation annotation : this.annotations) {
            if (annotationClazz.isInstance(annotation) && index >= annotation.getBeginIndex()
                    && index < annotation.getEndIndex()) {
                return annotationClazz.cast(annotation);
            }
        }
        return null;
    }

    public <T extends Annotation> T addAnnotation(final Class<T> annotationClazz,
            final int beginIndex, final int endIndex) {

        // Check parameters and lack of overlapping annotations
        Preconditions.checkState(this.text != null, "Post text not specified yet");
        Preconditions.checkArgument(beginIndex >= 0);
        Preconditions.checkArgument(endIndex <= this.text.length());
        Preconditions.checkNotNull(annotationClazz);
        Preconditions.checkArgument(annotationClazz != Annotation.class);

        // Check for an overlapping annotations: return it if it exactly matches the requested
        // annotation, otherwise throw an exception if not compatible
        for (final Annotation annotation : this.annotations) {
            if (annotation.getBeginIndex() < endIndex && annotation.getEndIndex() > beginIndex) {
                if (annotation.getBeginIndex() == beginIndex && annotation.getEndIndex() == endIndex
                        && annotationClazz.isInstance(annotation.getClass())) {
                    return annotationClazz.cast(annotation);
                }
                if (annotationClazz == EntityAnnotation.class
                        && annotation instanceof EntityAnnotation
                        || annotationClazz != EntityAnnotation.class
                                && !(annotation instanceof EntityAnnotation)) {
                    throw new IllegalArgumentException(
                            "Annotation already overlapping with inteval " + beginIndex + ","
                                    + endIndex + ": " + annotation);
                }
            }
        }

        // Create the new annotation
        Annotation annotation;
        if (annotationClazz == MentionAnnotation.class) {
            annotation = new MentionAnnotation(beginIndex, endIndex);
        } else if (annotationClazz == HashtagAnnotation.class) {
            annotation = new HashtagAnnotation(beginIndex, endIndex);
        } else if (annotationClazz == UrlAnnotation.class) {
            annotation = new UrlAnnotation(beginIndex, endIndex);
        } else if (annotationClazz == EntityAnnotation.class) {
            annotation = new EntityAnnotation(beginIndex, endIndex);
        } else {
            throw new IllegalArgumentException("Unknown annotation class: " + annotationClazz);
        }

        // Index and return the new annotation
        this.annotations.add(annotation);
        Collections.sort(this.annotations);
        return annotationClazz.cast(annotation);
    }

    public boolean removeAnnotation(final Annotation annotation) {
        return this.annotations.remove(Objects.requireNonNull(annotation));
    }

    @Nullable
    public Rewriting getRewriting() {
        return this.rewriting;
    }

    public void setRewriting(@Nullable final Rewriting rewriting) {
        this.rewriting = rewriting;
    }

    @Nullable
    public String getContext() {
        return this.context;
    }

    public void setContext(@Nullable final String context) {
        this.context = context;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Post)) {
            return false;
        }
        final Post other = (Post) object;
        return this.id == other.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("id", this.id);
        if (this.date != null) {
            json.addProperty("date", this.date.getTime() / 1000);
        }
        if (this.text != null) {
            json.addProperty("text", this.text);
        }
        final JsonObject author = new JsonObject();
        json.add("author", author);
        if (this.authorUsername != null) {
            author.addProperty("username", this.authorUsername);
        }
        if (this.authorFullName != null) {
            author.addProperty("fullName", this.authorFullName);
        }
        if (this.authorDescription != null) {
            author.addProperty("description", this.authorDescription);
        }
        if (this.authorUri != null) {
            author.addProperty("uri", this.authorUri);
        }
        final JsonArray annotations = new JsonArray();
        json.add("annotations", annotations);
        for (final Annotation annotation : this.annotations) {
            annotations.add(annotation.toJson());
        }
        if (this.rewriting != null) {
            final JsonObject r = this.rewriting.toJson();
            r.remove("from");
            json.add("rewriting", r);
        }
        if (this.context != null) {
            json.addProperty("context", this.context);
        }
        return json;
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public static List<Post> read(final Path path) throws IOException {
        final List<Post> posts = new ArrayList<>();
        try (Reader reader = IO.utf8Reader(IO.buffer(IO.read(path.toAbsolutePath().toString())))) {
            final Gson gson = new Gson();
            for (String line : CharStreams.readLines(reader)) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    try {
                        final Long id = Long.parseLong(line);
                        posts.add(new Post("twitter:" + id.toString()));
                    } catch (final NumberFormatException ex) {
                        final JsonObject json = gson.fromJson(line, JsonObject.class);
                        posts.add(new Post(json));
                    }
                }
            }
        }
        return posts;
    }

    public static void write(final Path path, final Iterable<Post> posts) throws IOException {
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(path.toAbsolutePath().toString())))) {
            final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            for (final Post post : posts) {
                final JsonObject json = post.toJson();
                writer.write(gson.toJson(json));
                writer.write("\n");
            }
        }
    }

    /**
     * A {@code Post} annotation.
     * <p>
     * An annotation refers to a specific span of the post text. It is characterized by begin and
     * end indexes (see {@link #getBeginIndex()} and {@link #getEndIndex()}), which encompass a
     * substring of the post text (see {@link #getText()}).
     * </p>
     * <p>
     * This is an abstract class. Different types of annotations with their attributes are defined
     * via subclasses (see {@link MentionAnnotation}, {@link HashtagAnnotation},
     * {@link UrlAnnotation}).
     * </p>
     */
    public abstract class Annotation implements Comparable<Annotation>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int beginIndex;

        private final int endIndex;

        @Nullable
        private final String text;

        Annotation(final JsonObject json) {
            this.beginIndex = json.get("begin").getAsInt();
            this.endIndex = json.get("end").getAsInt();
            this.text = Post.this.text.substring(this.beginIndex, this.endIndex);
        }

        Annotation(final int beginIndex, final int endIndex) {
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
            this.text = Post.this.text.substring(this.beginIndex, this.endIndex);
        }

        /**
         * Returns the {@code Post} this annotation refers to.
         *
         * @return the post
         */
        public Post getPost() {
            return Post.this;
        }

        /**
         * Returns the begin index for this annotation.
         *
         * @return the begin index
         */
        public int getBeginIndex() {
            return this.beginIndex;
        }

        /**
         * Returns the end index for this annotation.
         *
         * @return the end index
         */
        public int getEndIndex() {
            return this.endIndex;
        }

        /**
         * Returns the span of text annotated by this annotation.
         *
         * @return the annotated span of text
         */
        @Nullable
        public String getText() {
            return this.text;
        }

        /**
         * Returns a JSON representation of this annotation.
         *
         * @return the JSON object corresponding to this annotation
         */
        public JsonObject toJson() {
            final JsonObject json = new JsonObject();
            json.addProperty("begin", this.beginIndex);
            json.addProperty("end", this.endIndex);
            return json;
        }

        /**
         * {@inheritDoc} Annotations are sorted based on begin index.
         */
        @Override
        public int compareTo(final Annotation other) {
            return other == this ? 0 : this.beginIndex - other.beginIndex;
        }

        /**
         * {@inheritDoc} The returned string is based on the JSON representation by
         * {@link #toJson()}
         */
        @Override
        public String toString() {
            return toJson().toString();
        }

    }

    public final class MentionAnnotation extends Annotation {

        private static final long serialVersionUID = 1L;

        private final String username;

        @Nullable
        private String fullName;

        @Nullable
        private String description;

        @Nullable
        private String uri;

        MentionAnnotation(final JsonObject json) {
            super(json);
            this.username = json.get("username").getAsString();
            this.fullName = json.has("fullName") ? json.get("fullName").getAsString() : null;
            this.description = json.has("description") ? json.get("description").getAsString()
                    : null;
            this.uri = json.has("uri") ? json.get("uri").getAsString() : null;
        }

        MentionAnnotation(final int beginIndex, final int endIndex) {
            super(beginIndex, endIndex);
            this.username = getText().substring(1);
            this.fullName = null;
            this.description = null;
            this.uri = null;
        }

        public String getUsername() {
            return this.username;
        }

        @Nullable
        public String getFullName() {
            return this.fullName;
        }

        public void setFullName(@Nullable final String fullName) {
            this.fullName = fullName;
        }

        @Nullable
        public String getDescription() {
            return this.description;
        }

        public void setDescription(@Nullable final String description) {
            this.description = description;
        }

        @Nullable
        public String getUri() {
            return this.uri;
        }

        public void setUri(@Nullable final String uri) {
            this.uri = uri;
        }

        @Override
        public JsonObject toJson() {
            final JsonObject json = super.toJson();
            json.addProperty("username", this.username);
            if (this.fullName != null) {
                json.addProperty("fullName", this.fullName);
            }
            if (this.description != null) {
                json.addProperty("description", this.description);
            }
            if (this.uri != null) {
                json.addProperty("uri", this.uri);
            }
            return json;
        }

    }

    public final class HashtagAnnotation extends Annotation {

        private static final long serialVersionUID = 1L;

        private final String hashtag;

        @Nullable
        private String tokenization;

        @Nullable
        private Set<String> definitions;

        HashtagAnnotation(final JsonObject json) {
            super(json);
            this.hashtag = json.get("hashtag").getAsString();
            this.tokenization = json.has("tokenization") ? json.get("tokenization").getAsString()
                    : null;
            List<String> definitions = null;
            final JsonArray array = (JsonArray) json.get("definitions");
            if (array != null) {
                definitions = Lists.newArrayList();
                for (final JsonElement element : array) {
                    definitions.add(element.getAsString());
                }
            }
            this.definitions = definitions == null ? null : ImmutableSet.copyOf(definitions);
        }

        HashtagAnnotation(final int beginIndex, final int endIndex) {
            super(beginIndex, endIndex);
            this.hashtag = getText().substring(1);
            this.tokenization = null;
            this.definitions = null;
        }

        public String getHashtag() {
            return this.hashtag;
        }

        @Nullable
        public String getTokenization() {
            return this.tokenization;
        }

        public void setTokenization(@Nullable final String tokenization) {
            this.tokenization = tokenization;
        }

        @Nullable
        public Set<String> getDefinitions() {
            return this.definitions;
        }

        public void setDefinitions(@Nullable final Iterable<String> definitions) {
            this.definitions = definitions == null ? null : ImmutableSet.copyOf(definitions);
        }

        @Override
        public JsonObject toJson() {
            final JsonObject json = super.toJson();
            json.addProperty("hashtag", this.hashtag);
            if (this.tokenization != null) {
                json.addProperty("tokenization", this.tokenization);
            }
            if (this.definitions != null) {
                final JsonArray array = new JsonArray();
                for (final String definition : Ordering.natural().sortedCopy(this.definitions)) {
                    array.add(definition);
                }
                json.add("definitions", array);
            }
            return json;
        }

    }

    public final class UrlAnnotation extends Annotation {

        private static final long serialVersionUID = 1L;

        private String resolvedUrl;

        private String title;

        UrlAnnotation(final JsonObject json) {
            super(json);
            this.resolvedUrl = json.has("resolvedUrl") ? json.get("resolvedUrl").getAsString()
                    : null;
            this.title = json.has("title") ? json.get("title").getAsString() : null;
        }

        UrlAnnotation(final int beginIndex, final int endIndex) {
            super(beginIndex, endIndex);
            this.resolvedUrl = null;
            this.title = null;
        }

        public String getUrl() {
            return getText();
        }

        @Nullable
        public String getResolvedUrl() {
            return this.resolvedUrl;
        }

        public void setResolvedUrl(@Nullable final String resolvedUrl) {
            this.resolvedUrl = resolvedUrl;
        }

        @Nullable
        public String getTitle() {
            return this.title;
        }

        public void setTitle(@Nullable final String title) {
            this.title = title;
        }

        @Override
        public JsonObject toJson() {
            final JsonObject json = super.toJson();
            json.addProperty("url", getUrl());
            if (this.resolvedUrl != null) {
                json.addProperty("resolvedUrl", this.resolvedUrl);
            }
            if (this.title != null) {
                json.addProperty("title", this.title);
            }
            return json;
        }

    }

    public final class EntityAnnotation extends Annotation {

        private static final long serialVersionUID = 1L;

        private Category category;

        private String uri;

        EntityAnnotation(final JsonObject json) {
            super(json);
            this.category = json.has("category")
                    ? Category.valueOf(json.get("category").getAsString().toUpperCase().trim())
                    : null;
            this.uri = json.has("uri") ? json.get("uri").getAsString() : null;
        }

        EntityAnnotation(final int beginIndex, final int endIndex) {
            super(beginIndex, endIndex);
        }

        public String getSurfaceForm() {
            return getText();
        }

        @Nullable
        public Category getCategory() {
            return this.category;
        }

        public void setCategory(@Nullable final Category category) {
            this.category = category;
        }

        @Nullable
        public String getUri() {
            return this.uri;
        }

        public void setUri(@Nullable final String uri) {
            this.uri = uri;
        }

        @Override
        public JsonObject toJson() {
            final JsonObject json = super.toJson();
            json.addProperty("surfaceForm", getSurfaceForm());
            if (this.category != null) {
                json.addProperty("category", this.category.toString().toLowerCase());
            }
            if (this.uri != null) {
                json.addProperty("uri", this.uri);
            }
            return json;
        }

    }

}
