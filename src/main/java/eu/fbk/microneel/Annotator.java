package eu.fbk.microneel;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public interface Annotator {

    static final Annotator NIL = new Annotator() {

        @Override
        public void annotate(final Post post) throws Throwable {
            // Do nothing
        }

        @Override
        public void annotate(final Iterable<Post> posts) throws Throwable {
            // Do nothing
        }

        @Override
        public String toString() {
            return "NIL";
        }

    };

    default void annotate(final Post post) throws Throwable {
        annotate(() -> Iterators.singletonIterator(post));
    }

    default void annotate(final Iterable<Post> posts) throws Throwable {
        if (posts.getClass().getName().startsWith(Annotator.class.getName())) {
            throw new Error("At least one annotator method should be overridden!");
        }
        for (final Post post : posts) {
            annotate(post);
        }
    }

    static Annotator sequence(final Annotator... annotators) {
        if (annotators.length == 0) {
            return NIL;
        } else if (annotators.length == 1) {
            return annotators[0];
        } else {
            return new Annotator() {

                private final Annotator[] theAnnotators = annotators.clone();

                @Override
                public void annotate(final Post post) throws Throwable {
                    for (final Annotator annotator : this.theAnnotators) {
                        annotator.annotate(post);
                    }
                }

                @Override
                public void annotate(final Iterable<Post> posts) throws Throwable {
                    for (final Annotator annotator : this.theAnnotators) {
                        annotator.annotate(posts);
                    }
                }

                @Override
                public String toString() {
                    return "sequence(" + Joiner.on(", ").join(annotators) + ")";
                }

            };
        }
    }

    static Annotator parallel(final Annotator... annotators) {
        if (annotators.length == 0) {
            return NIL;
        } else if (annotators.length == 1) {
            return annotators[0];
        } else {
            return new Annotator() {

                private final Annotator[] theAnnotators = annotators.clone();

                @Override
                public void annotate(final Post post) throws Throwable {
                    final Post[] copies = new Post[this.theAnnotators.length];
                    copies[0] = post;
                    for (int i = 1; i < this.theAnnotators.length; ++i) {
                        copies[i] = post.clone();
                    }
                    for (int i = 0; i < this.theAnnotators.length; ++i) {
                        this.theAnnotators[i].annotate(copies[i]);
                    }
                    for (int i = 1; i < this.theAnnotators.length; ++i) {
                        post.merge(copies[i]);
                    }
                }

                @Override
                public void annotate(final Iterable<Post> posts) throws Throwable {
                    final List<Post> list = ImmutableList.copyOf(posts);
                    final List<List<Post>> copies = new ArrayList<>();
                    copies.add(list);
                    for (int i = 1; i < this.theAnnotators.length; ++i) {
                        final List<Post> copy = Lists.newArrayListWithCapacity(list.size());
                        for (final Post post : list) {
                            copy.add(post.clone());
                        }
                        copies.add(copy);
                    }
                    for (int i = 0; i < this.theAnnotators.length; ++i) {
                        this.theAnnotators[i].annotate(copies.get(i));
                    }
                    for (int i = 1; i < this.theAnnotators.length; ++i) {
                        final List<Post> copy = copies.get(i);
                        for (int j = 0; j < copies.get(0).size(); ++j) {
                            list.get(j).merge(copy.get(j));
                        }
                    }
                }

                @Override
                public String toString() {
                    return "parallel(" + Joiner.on(", ").join(annotators) + ")";
                }

            };
        }
    }

    static Annotator create(final JsonObject json, @Nullable Path path) {

        // Use working dir if path was not supplied
        if (path == null) {
            path = Paths.get(System.getProperty("user.dir"));
        }

        // Build/return different kinds of Annotator based on 'type' field of JSON object
        final String type = json.get("type").getAsString();
        if (type == null || type.equalsIgnoreCase("nil")) {
            // Return NIL annotator
            return NIL;

        } else if (type.equalsIgnoreCase("sequence") || type.equalsIgnoreCase("parallel")) {
            // Create either a 'sequence' or 'parallel' annotator wrapping a list of annotators
            final List<Annotator> annotators = new ArrayList<>();
            final JsonArray array = (JsonArray) json.get("annotators");
            if (array != null) {
                for (final JsonElement element : array) {
                    annotators.add(create((JsonObject) element, path));
                }
            }
            final Annotator[] a = annotators.toArray(new Annotator[annotators.size()]);
            return type.equalsIgnoreCase("sequence") ? sequence(a) : parallel(a);

        } else {
            // Instantiate a custom annotator via reflection
            try {
                final Class<?> clazz = Class.forName(type);
                if (!Annotator.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException("Not an Annotator class: " + type);
                }
                try {
                    final Field field = clazz.getField("INSTANCE");
                    if (Modifier.isStatic(field.getModifiers())
                            && Annotator.class.isAssignableFrom(field.getType())) {
                        return (Annotator) field.get(null);
                    }
                } catch (final IllegalAccessException ex) {
                    throw new IllegalArgumentException(
                            "Cannot access field INSTANCE of type " + type);
                } catch (final NoSuchFieldException ex) {
                    // ignore
                }
                try {
                    Constructor<?> constructor;
                    try {
                        constructor = clazz.getConstructor(JsonObject.class);
                        return (Annotator) constructor.newInstance(json);
                    } catch (final NoSuchMethodException ex) {
                        constructor = clazz.getConstructor(JsonObject.class, Path.class);
                        return (Annotator) constructor.newInstance(json, path);
                    }
                } catch (final InvocationTargetException ex) {
                    throw Throwables.propagate(ex.getCause());
                } catch (final IllegalAccessException | InstantiationException ex) {
                    throw new IllegalArgumentException("Cannot instantiate type " + type
                            + " using constructor taking a JsonObject", ex);
                } catch (final NoSuchMethodException ex) {
                    throw new IllegalArgumentException("Annotator class " + type
                            + " does not define a suitable constructor nor a static INSTANCE field");
                }
            } catch (final ClassNotFoundException ex) {
                throw new IllegalArgumentException("Class does not exist: " + type);
            }
        }
    }

}
