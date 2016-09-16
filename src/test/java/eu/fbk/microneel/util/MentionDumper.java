package eu.fbk.microneel.util;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.utils.core.IO;

public class MentionDumper {

    public static void main(final String... args) throws IOException {

        final Path datasetPath = Paths.get(args[0]);
        final Path postsPath = Paths.get(args[1]);
        final Path entitiesPath = args.length > 2 ? Paths.get(args[2]) : null;

        Multimap<Long, Entity> entities = null;
        if (entitiesPath != null) {
            entities = HashMultimap.create();
            for (final String line : Files.readAllLines(entitiesPath)) {
                final String[] fields = line.split("\t");
                final long tweetId = Long.parseLong(fields[0]);
                final int beginIndex = Integer.parseInt(fields[1]);
                final int endIndex = Integer.parseInt(fields[2]);
                final Category category = Category.valueOf(fields[4].toUpperCase());
                entities.put(tweetId, new Entity(tweetId, beginIndex, endIndex, category));
            }
        }

        final Map<String, Category> userCategories = Maps.newHashMap();
        final Map<String, String> userNames = Maps.newHashMap();
        final Map<String, String> userDescriptions = Maps.newHashMap();
        final Map<String, String> userLangs = Maps.newHashMap();
        final Map<String, String> userUris = Maps.newHashMap();
        final Map<String, Category> userUriCategories = Maps.newHashMap();

        for (final Post post : Post.read(postsPath)) {
            final Collection<Entity> postEntities = entities == null ? null
                    : entities.get(post.getTwitterId());
            for (final MentionAnnotation a : post.getAnnotations(MentionAnnotation.class)) {
                userCategories.putIfAbsent(a.getUsername(), null);
                userNames.putIfAbsent(a.getUsername(), a.getFullName());
                userDescriptions.putIfAbsent(a.getUsername(), a.getDescription());
                userLangs.putIfAbsent(a.getUsername(), a.getLang());
                userUris.putIfAbsent(a.getUsername(), a.getUri());
                userUriCategories.putIfAbsent(a.getUsername(), a.getCategory());
                if (postEntities != null) {
                    for (final Entity e : postEntities) {
                        if (e.endIndex > a.getBeginIndex() && e.beginIndex < a.getEndIndex()) {
                            // System.out.println(e.beginIndex - a.getBeginIndex() + " - "
                            // + (e.endIndex - a.getEndIndex()));
                            userCategories.put(a.getUsername(), e.category);
                            break;
                        }
                    }
                }
            }
        }

        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(datasetPath.toString())))) {
            writer.write(
                    "screen_name\tcategory\tfull_name\tdescription\tlang\turi\turi_category\n");
            // writer.write("screen_name\tcategory\n");
            for (final String username : Ordering.natural().sortedCopy(userCategories.keySet())) {
                final String name = userNames.get(username);
                final String description = userDescriptions.get(username);
                final String lang = userLangs.get(username);
                final String uri = userUris.get(username);
                final Category uriCategory = userUriCategories.get(username);
                final Category category = userCategories.get(username);
                writer.write(username);
                writer.write('\t');
                writer.write(category == null ? "Null"
                        : category == Category.PERSON ? "Person" : "Organization");
                writer.write('\t');
                writer.write(name == null ? "Null" : name.replace('\n', ' '));
                writer.write('\t');
                writer.write(description == null ? "Null" : description.replace('\n', ' '));
                writer.write('\t');
                writer.write(lang == null ? "Null" : lang);
                writer.write('\t');
                writer.write(uri == null ? "Null" : uri);
                writer.write('\t');
                writer.write(uriCategory == null ? "Null"
                        : category == Category.PERSON ? "Person" : "Organization");
                writer.write('\n');
            }
        }
    }

    private static final class Entity {

        final long tweetId;

        final int beginIndex;

        final int endIndex;

        final Category category;

        Entity(final long tweetId, final int beginIndex, final int endIndex,
                final Category category) {
            this.tweetId = tweetId;
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
            this.category = category;
        }

    }

}
