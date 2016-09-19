package eu.fbk.microneel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.CharStreams;
import com.google.gson.*;
import eu.fbk.microneel.util.Rewriting;
import eu.fbk.utils.core.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Post implements Serializable, Cloneable, Comparable<Post> {

    public static final String DEFAULT_QUALIFIER = "";

    private static final Logger LOGGER = LoggerFactory.getLogger(Post.class);

    private static final Pattern MENTION_PATTERN = Pattern
            .compile("(^|[^A-Za-z0-9_])@([A-Za-z0-9_]+)($|[^A-Za-z0-9_])");

    private static final Pattern HASHTAG_PATTERN = Pattern.compile(
            "(^|[^0-9_\\p{IsAlphabetic}])#([0-9]*[A-Za-z][0-9_\\p{IsAlphabetic}]+)($|[^0-9_\\p{IsAlphabetic}])");

    private static final Pattern URL_PATTERN = Pattern
            .compile("(^|[^A-Za-z0-9_])(http|https)://t.co/([A-Za-z0-9_]+)($|[^A-Za-z0-9_])");

    private static final long serialVersionUID = 1L;

    private final String id;

    @Nullable
    private Date date;

    @Nullable
    private String text;

    @Nullable
    private String lang;

    @Nullable
    private String authorUsername;

    @Nullable
    private String authorFullName;

    @Nullable
    private String authorDescription;

    @Nullable
    private String authorLang;

    @Nullable
    private Category authorCategory;

    @Nullable
    private String authorUri;

    private final List<Annotation> annotations;

    @Nullable
    private Rewriting rewriting;

    public Post(final JsonObject json) {
        this.id = json.get("id").getAsString();
        this.date = json.has("date") ? new Date(json.get("date").getAsLong() * 1000) : null;
        this.text = json.has("text") ? json.get("text").getAsString() : null;
        this.lang = json.has("lang") ? json.get("lang").getAsString().toLowerCase() : null;
        final JsonObject author = json.getAsJsonObject("author");
        this.authorUsername = author.has("username") ? author.get("username").getAsString() : null;
        this.authorFullName = author.has("fullName") ? author.get("fullName").getAsString() : null;
        this.authorDescription = author.has("description") ? author.get("description").getAsString()
                : null;
        this.authorLang = author.has("lang") ? author.get("lang").getAsString() : null;
        this.authorCategory = author.has("category")
                ? Category.valueOf(author.get("category").getAsString().toUpperCase()) : null;
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
            } else if (e.has("surfaceForm")) {
                this.annotations.add(new EntityAnnotation(e));
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
    }

    public Post(final String id) {
        this.id = Objects.requireNonNull(id);
        this.date = null;
        this.text = null;
        this.lang = null;
        this.authorUsername = null;
        this.authorFullName = null;
        this.authorDescription = null;
        this.authorLang = null;
        this.authorCategory = null;
        this.authorUri = null;
        this.annotations = new ArrayList<>();
        this.rewriting = null;
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
            this.text = text;
            if (text == null) {
                this.annotations.clear();
            } else {
                // Remove illegal annotations
                for (final Iterator<Annotation> i = this.annotations.iterator(); i.hasNext();) {
                    final Annotation annotation = i.next();
                    if (annotation.getEndIndex() > text.length() || !annotation.getText().equals(
                            text.substring(annotation.getBeginIndex(), annotation.getEndIndex()))) {
                        i.remove();
                    }
                }

                // Detect mentions in the text
                Matcher m = MENTION_PATTERN.matcher(text);
                for (int start = 0; m.find(start); start = m.end() - 1) {
                    final int begin = m.start(2) - 1;
                    final int end = m.end(2);
                    addAnnotation(MentionAnnotation.class, begin, end);
                }

                // Detect hashtags in the text
                m = HASHTAG_PATTERN.matcher(text);
                for (int start = 0; m.find(start); start = m.end() - 1) {
                    final int begin = m.start(2) - 1;
                    final int end = m.end(2);
                    addAnnotation(HashtagAnnotation.class, begin, end);
                }

                // Detect URLs in the text
                m = URL_PATTERN.matcher(text);
                for (int start = 0; m.find(start); start = m.end() - 1) {
                    final int begin = m.start(2);
                    final int end = m.end(3);
                    addAnnotation(UrlAnnotation.class, begin, end);
                }
            }
        }
    }

    @Nullable
    public String getLang() {
        return this.lang;
    }

    public void setLang(@Nullable final String lang) {
        this.lang = lang;
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
    public String getAuthorLang() {
        return this.authorLang;
    }

    public void setAuthorLang(@Nullable final String authorLang) {
        this.authorLang = authorLang;
    }

    @Nullable
    public Category getAuthorCategory() {
        return this.authorCategory;
    }

    public void setAuthorCategory(@Nullable final Category authorCategory) {
        this.authorCategory = authorCategory;
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
        return getAnnotations(annotationClazz, DEFAULT_QUALIFIER);
    }

    public <T extends Annotation> List<T> getAnnotations(final Class<T> annotationClazz,
            @Nullable final String qualifier) {
        Objects.requireNonNull(annotationClazz);
        final List<T> result = new ArrayList<>();
        for (final Annotation annotation : this.annotations) {
            if (annotationClazz.isInstance(annotation)
                    && (qualifier == null || qualifier.equals(annotation.getQualifier()))) {
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

    public <T extends Annotation> T getAnnotations(final int index,
            final Class<T> annotationClazz) {
        return getAnnotation(index, annotationClazz, DEFAULT_QUALIFIER);
    }

    public <T extends Annotation> T getAnnotation(final int index, final Class<T> annotationClazz,
            final String qualifier) {
        Objects.requireNonNull(qualifier);
        for (final Annotation annotation : this.annotations) {
            if (annotationClazz.isInstance(annotation) && index >= annotation.getBeginIndex()
                    && index < annotation.getEndIndex()
                    && qualifier.equals(annotation.getQualifier())) {
                return annotationClazz.cast(annotation);
            }
        }
        return null;
    }

    public <T extends Annotation> T addAnnotation(final Class<T> annotationClazz,
            final int beginIndex, final int endIndex) {
        return addAnnotation(annotationClazz, beginIndex, endIndex, DEFAULT_QUALIFIER);
    }

    public <T extends Annotation> T addAnnotation(final Class<T> annotationClazz,
            final int beginIndex, final int endIndex, final String qualifier) {

        // Check parameters and lack of overlapping annotations
        Preconditions.checkState(this.text != null, "Post text not specified yet");
        Preconditions.checkArgument(beginIndex >= 0);
        Preconditions.checkArgument(endIndex <= this.text.length());
        Preconditions.checkArgument(qualifier != null);
        Preconditions.checkNotNull(annotationClazz);
        Preconditions.checkArgument(annotationClazz != Annotation.class);

        // Check for an overlapping annotations: return it if it exactly matches the requested
        // annotation, otherwise throw an exception if not compatible
        for (final Annotation annotation : this.annotations) {
            if (annotation.getBeginIndex() < endIndex && annotation.getEndIndex() > beginIndex) {
                final boolean sameClass = annotationClazz == annotation.getClass();
                final boolean sameQualifier = annotation.getQualifier().equals(qualifier);
                if (annotation.getBeginIndex() == beginIndex && annotation.getEndIndex() == endIndex
                        && sameClass && sameQualifier) {
                    return annotationClazz.cast(annotation);
                }
                if (sameClass && sameQualifier
                        || !sameClass && annotationClazz != EntityAnnotation.class
                                && annotation.getClass() != EntityAnnotation.class) {
                    throw new IllegalStateException("Cannot annotate " + beginIndex + ", "
                            + endIndex + " with a " + annotationClazz.getSimpleName()
                            + " as interval overlaps with " + annotation);
                }
            }
        }

        // Create the new annotation
        Annotation annotation;
        if (annotationClazz == MentionAnnotation.class) {
            annotation = new MentionAnnotation(beginIndex, endIndex, qualifier);
        } else if (annotationClazz == HashtagAnnotation.class) {
            annotation = new HashtagAnnotation(beginIndex, endIndex, qualifier);
        } else if (annotationClazz == UrlAnnotation.class) {
            annotation = new UrlAnnotation(beginIndex, endIndex, qualifier);
        } else if (annotationClazz == EntityAnnotation.class) {
            annotation = new EntityAnnotation(beginIndex, endIndex, qualifier);
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

    @Override
    public Post clone() {
        final Post clone = new Post(this.id);
        clone.merge(this);
        return clone;
    }

    public void merge(final Post post) {

        // Import date if missing in this post
        if (this.date == null) {
            this.date = post.date;
        }

        // Import author data from supplied post, if compatible with author data in this post
        if (compatible(this.authorUsername, post.authorUsername)
                && compatible(this.authorFullName, post.authorFullName)
                && compatible(this.authorDescription, post.authorDescription)
                && compatible(this.authorLang, post.authorLang)
                && compatible(this.authorCategory, post.authorCategory)
                && compatible(this.authorUri, post.authorUri)) {
            if (this.authorUsername == null) {
                this.authorUsername = post.authorUsername;
            }
            if (this.authorFullName == null) {
                this.authorFullName = post.authorFullName;
            }
            if (this.authorDescription == null) {
                this.authorDescription = post.authorDescription;
            }
            if (this.authorLang == null) {
                this.authorLang = post.authorLang;
            }
            if (this.authorCategory == null) {
                this.authorCategory = post.authorCategory;
            }
            if (this.authorUri == null) {
                this.authorUri = post.authorUri;
            }
        }

        // Import other data from supplied post, if text and language are compatible
        if (compatible(this.text, post.text) && compatible(this.lang, post.lang)) {

            // Import text and language
            if (this.text == null) {
                this.text = post.text;
            }
            if (this.lang == null) {
                this.lang = post.lang;
            }

            // Import rewriting, if missing here
            if (this.rewriting == null && post.rewriting != null) {
                this.rewriting = post.rewriting.clone();
            }

            // Import compatible annotations
            for (final Annotation annotation : post.annotations) {
                try {
                    if (annotation instanceof MentionAnnotation) {
                        final MentionAnnotation pa = (MentionAnnotation) annotation;
                        final MentionAnnotation ta = addAnnotation(MentionAnnotation.class,
                                pa.getBeginIndex(), pa.getEndIndex(), pa.getQualifier());
                        if (compatible(ta.getFullName(), pa.getFullName())
                                && compatible(ta.getDescription(), pa.getDescription())
                                && compatible(ta.getLang(), pa.getLang())
                                && compatible(ta.getUri(), pa.getUri())) {
                            if (ta.getFullName() == null) {
                                ta.setFullName(pa.getFullName());
                            }
                            if (ta.getDescription() == null) {
                                ta.setDescription(pa.getDescription());
                            }
                            if (ta.getLang() == null) {
                                ta.setLang(pa.getLang());
                            }
                            if (ta.getUri() == null) {
                                ta.setUri(pa.getUri());
                            }
                        }
                    } else if (annotation instanceof HashtagAnnotation) {
                        final HashtagAnnotation pa = (HashtagAnnotation) annotation;
                        final HashtagAnnotation ta = addAnnotation(HashtagAnnotation.class,
                                pa.getBeginIndex(), pa.getEndIndex(), pa.getQualifier());
                        if (compatible(ta.getTokenization(), pa.getTokenization())) {
                            ta.setTokenization(pa.getTokenization());
                            if (ta.getDefinitions().isEmpty()) {
                                ta.setDefinitions(pa.getDefinitions());
                            } else if (!pa.getDefinitions().isEmpty()) {
                                final List<String> definitions = Lists
                                        .newArrayList(ta.getDefinitions());
                                definitions.addAll(pa.getDefinitions());
                                ta.setDefinitions(ImmutableSet.copyOf(definitions));
                            }
                        }
                    } else if (annotation instanceof UrlAnnotation) {
                        final UrlAnnotation pa = (UrlAnnotation) annotation;
                        final UrlAnnotation ta = addAnnotation(UrlAnnotation.class,
                                pa.getBeginIndex(), pa.getEndIndex(), pa.getQualifier());
                        if (compatible(ta.getResolvedUrl(), pa.getResolvedUrl())
                                && compatible(ta.getTitle(), pa.getTitle())) {
                            if (ta.getResolvedUrl() == null) {
                                ta.setResolvedUrl(pa.getResolvedUrl());
                            }
                            if (ta.getTitle() == null) {
                                ta.setTitle(pa.getTitle());
                            }
                        }
                    } else if (annotation instanceof EntityAnnotation) {
                        final EntityAnnotation pa = (EntityAnnotation) annotation;
                        final EntityAnnotation ta = addAnnotation(EntityAnnotation.class,
                                pa.getBeginIndex(), pa.getEndIndex(), pa.getQualifier());
                        if (compatible(ta.getCategory(), pa.getCategory())
                                && compatible(ta.getUri(), pa.getUri())) {
                            if (ta.getCategory() == null) {
                                ta.setCategory(pa.getCategory());
                            }
                            if (ta.getUri() == null) {
                                ta.setUri(pa.getUri());
                            }
                        }
                    }
                } catch (final IllegalStateException ex) {
                    // ignore: annotation not compatible with the ones in this post
                }
            }
        }
    }

    private static <T> boolean compatible(final T first, final T second) {
        return first == null || second == null || first.equals(second);
    }

    @Override
    public int compareTo(final Post other) {
        return this.id.compareTo(other.id);
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
        if (this.lang != null) {
            json.addProperty("lang", this.lang);
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
        if (this.authorLang != null) {
            author.addProperty("lang", this.authorLang);
        }
        if (this.authorCategory != null) {
            author.addProperty("category", this.authorCategory.toString().toLowerCase());
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
                        final JsonObject json = gson.fromJson(line, JsonObject.class);
                        posts.add(new Post(json));
                    } catch (final Throwable ex) {
                        try {
                            final String[] fields = line.split("\t");
                            if (fields.length <= 2) {
                                final Post post = new Post(
                                        "twitter:" + Long.valueOf(fields[0].trim()).toString());
                                if (fields.length == 2) {
                                    post.setText(fields[1]);
                                }
                                posts.add(post);
                            }
                        } catch (final Throwable ex2) {
                            LOGGER.warn("Cannot read line parsed as JSON: " + line, ex);
                            LOGGER.warn("Cannot read line parsed as TSV: " + line, ex2);
                        }
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

        private final String qualifier;

        @Nullable
        private final String text;

        Annotation(final JsonObject json) {
            this.beginIndex = json.get("begin").getAsInt();
            this.endIndex = json.get("end").getAsInt();
            this.qualifier = json.has("q") ? json.get("q").getAsString() : DEFAULT_QUALIFIER;
            this.text = Post.this.text.substring(this.beginIndex, this.endIndex);
        }

        Annotation(final int beginIndex, final int endIndex, final String qualifier) {
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
            this.qualifier = qualifier;
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
         * Returns a qualifier further characterizing the type of annotation (default empty string).
         *
         * @return the qualifier
         */
        public String getQualifier() {
            return this.qualifier;
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
            if (!Strings.isNullOrEmpty(this.qualifier)) {
                json.addProperty("q", this.qualifier);
            }
            return json;
        }

        /**
         * {@inheritDoc} Annotations are sorted based on begin index.
         */
        @Override
        public int compareTo(final Annotation other) {
            int result = this.beginIndex - other.beginIndex;
            if (result == 0) {
                result = this.endIndex - other.endIndex;
                if (result == 0) {
                    result = this.getClass().getName().compareTo(other.getClass().getName());
                    if (result == 0) {
                        result = this.qualifier.compareTo(other.qualifier);
                        if (result == 0) {
                            result = System.identityHashCode(this) - System.identityHashCode(other);
                        }
                    }
                }
            }
            return result;
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
        private String lang;

        @Nullable
        private Category category;

        @Nullable
        private String uri;

        MentionAnnotation(final JsonObject json) {
            super(json);
            this.username = json.get("username").getAsString();
            this.fullName = json.has("fullName") ? json.get("fullName").getAsString() : null;
            this.description = json.has("description") ? json.get("description").getAsString()
                    : null;
            this.lang = json.has("lang") ? json.get("lang").getAsString() : null;
            this.category = json.has("category")
                    ? Category.valueOf(json.get("category").getAsString().toUpperCase()) : null;
            this.uri = json.has("uri") ? json.get("uri").getAsString() : null;
        }

        MentionAnnotation(final int beginIndex, final int endIndex, final String qualifier) {
            super(beginIndex, endIndex, qualifier);
            this.username = getText().substring(1);
            this.fullName = null;
            this.description = null;
            this.lang = null;
            this.category = null;
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
        public String getLang() {
            return this.lang;
        }

        public void setLang(@Nullable final String lang) {
            this.lang = lang;
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
            json.addProperty("username", this.username);
            if (this.fullName != null) {
                json.addProperty("fullName", this.fullName);
            }
            if (this.description != null) {
                json.addProperty("description", this.description);
            }
            if (this.lang != null) {
                json.addProperty("lang", this.lang);
            }
            if (this.category != null) {
                json.addProperty("category", this.category.toString().toLowerCase());
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

        HashtagAnnotation(final int beginIndex, final int endIndex, final String qualifier) {
            super(beginIndex, endIndex, qualifier);
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

        UrlAnnotation(final int beginIndex, final int endIndex, final String qualifier) {
            super(beginIndex, endIndex, qualifier);
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
        private Integer beginIndexRewritten = null, endIndexRewritten = null;

        public Integer getBeginIndexRewritten() {
            return beginIndexRewritten;
        }

        public void setBeginIndexRewritten(Integer beginIndexRewritten) {
            this.beginIndexRewritten = beginIndexRewritten;
        }

        public Integer getEndIndexRewritten() {
            return endIndexRewritten;
        }

        public void setEndIndexRewritten(Integer endIndexRewritten) {
            this.endIndexRewritten = endIndexRewritten;
        }

        EntityAnnotation(final JsonObject json) {
            super(json);
            this.category = json.has("category")
                    ? Category.valueOf(json.get("category").getAsString().toUpperCase().trim())
                    : null;
            this.beginIndexRewritten = json.has("beginIndexRewritten")
                    ? json.get("beginIndexRewritten").getAsInt() : null;
            this.endIndexRewritten = json.has("endIndexRewritten")
                    ? json.get("endIndexRewritten").getAsInt() : null;
            this.uri = json.has("uri") ? json.get("uri").getAsString() : null;
        }

        EntityAnnotation(final int beginIndex, final int endIndex, final String qualifier) {
            super(beginIndex, endIndex, qualifier);
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
            if (this.beginIndexRewritten != null) {
                json.addProperty("beginIndexRewritten", this.beginIndexRewritten);
            }
            if (this.endIndexRewritten != null) {
                json.addProperty("endIndexRewritten", this.endIndexRewritten);
            }
            return json;
        }

    }

}
