package eu.fbk.microneel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.microneel.util.TwitterBuilder;
import eu.fbk.utils.core.CommandLine;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

public abstract class Enricher {

    private static final Logger LOGGER = LoggerFactory.getLogger(Enricher.class);

    public void enrich(final Post post) throws Throwable {
        enrich(ImmutableList.of(post));
    }

    public abstract void enrich(Iterable<Post> posts) throws Throwable;

    public static Enricher concat(final Enricher... enrichers) {
        if (enrichers.length == 0) {
            return createNullEnricher();
        } else if (enrichers.length == 1) {
            return enrichers[0];
        } else {
            return new ConcatEnricher(enrichers);
        }
    }

    public static Enricher createNullEnricher() {
        return NullEnricher.INSTANCE;
    }

    public static Enricher createTwitterApiEnricher(final Twitter twitter) {
        return new TwitterApiEnricher(Objects.requireNonNull(twitter));
    }

    public static Enricher create(final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = Strings.isNullOrEmpty(prefix) ? "" : prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of enrichers to be later combined
        final List<Enricher> enrichers = new ArrayList<>();

        // Retrieve the types of enrichers enabled in the configuration
        final Set<String> types = ImmutableSet
                .copyOf(properties.getProperty(prefix + "type", "").split("\\s+"));

        // Add an enricher adding triples about certain URIs, possibly recursively
        if (types.contains("api")) {
            enrichers.add(createTwitterApiEnricher(
                    new TwitterBuilder().setProperties(properties, prefix + "api.").build()));
        }

        // Combine the enrichers
        return concat(enrichers.toArray(new Enricher[enrichers.size()]));
    }

    public static void main(final String... args) {
        try {
            // Parse command line
            final long ts = System.currentTimeMillis();
            final CommandLine cmd = CommandLine.parser().withName("microneel-enricher")
                    .withHeader("Enriches a set of microposts.")
                    .withOption("c", "config",
                            "specifies the configuration file (default: microneel.properties)",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption(null, "config-prefix",
                            "specifies the PREFIX of configuration properties to use "
                                    + "(default: enricher.)",
                            "PREFIX", CommandLine.Type.STRING, true, false, false)
                    .withOption("i", "input", "specifies the input FILE with the posts to enrich",
                            "FILE", CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withOption("o", "output",
                            "specifies the output FILE populated with the enriched tweets", "FILE",
                            CommandLine.Type.FILE, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read options
            final File configFile = cmd.getOptionValue("c", File.class,
                    new File("crawler.properties"));
            final String configPrefix = cmd.getOptionValue("config-prefix", String.class,
                    "enricher.");
            final Path inputPath = cmd.getOptionValue("i", Path.class);
            final Path outputPath = cmd.getOptionValue("o", Path.class);

            // Read configuration
            final Properties config = new Properties();
            try (Reader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(configFile), Charset.forName("UTF-8")))) {
                config.load(reader);
                LOGGER.info("Loaded configuration from {}", configFile);
            }

            // Create the enricher based on the supplied configuration
            final Enricher enricher = Enricher.create(config, configPrefix);
            LOGGER.info("Configured {}", enricher);

            // Read posts
            final List<Post> posts = Post.read(inputPath);
            LOGGER.info("Read {} posts from {}", posts.size(), inputPath);

            // Perform the enrichment
            enricher.enrich(posts);
            LOGGER.info("Enriched {} posts", posts.size());

            // Write results
            Post.write(outputPath, posts);
            LOGGER.info("Written {} posts to {}", posts.size(), outputPath);
            LOGGER.info("Done in {} ms", System.currentTimeMillis() - ts);

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }
    }

    private static class ConcatEnricher extends Enricher {

        private final Enricher[] enrichers;

        ConcatEnricher(final Enricher[] enrichers) {
            this.enrichers = enrichers;
        }

        @Override
        public void enrich(final Iterable<Post> posts) throws Throwable {
            for (final Enricher enricher : this.enrichers) {
                enricher.enrich(posts);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + Joiner.on(", ").join(this.enrichers) + ")";
        }

    }

    private static class NullEnricher extends Enricher {

        static final NullEnricher INSTANCE = new NullEnricher();

        @Override
        public void enrich(final Iterable<Post> posts) throws Throwable {
            // do nothing
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

    }

    private static class TwitterApiEnricher extends Enricher {

        private final Twitter twitter;

        TwitterApiEnricher(final Twitter twitter) {
            this.twitter = twitter;
        }

        @Override
        public void enrich(final Iterable<Post> posts) throws TwitterException {
            enrichViaStatusLookup(posts);
            enrichViaUserLookup(posts);
        }

        private void enrichViaStatusLookup(final Iterable<Post> posts) throws TwitterException {

            // Collect the IDs of the statuses that is possible & useful to gather from Twitter
            final Set<Long> ids = new HashSet<>();
            for (final Post post : posts) {
                final Long id = post.getTwitterId();
                if (id != null && (post.getDate() == null || post.getText() == null
                        || post.getAuthorUsername() == null || post.getAuthorFullName() == null
                        || post.getAuthorDescription() == null
                        || post.getAnnotations(MentionAnnotation.class).stream()
                                .anyMatch(a -> a.getFullName() == null))) {
                    ids.add(id);
                }
            }

            // Gather statuses in batches of 100 (most efficient way)
            final Map<Long, Status> statuses = new HashMap<>();
            for (final List<Long> batch : Iterables.partition(ids, 100)) {
                for (final Status status : this.twitter.lookup(Longs.toArray(batch))) {
                    statuses.put(status.getId(), status);
                }
            }

            // Use retrieved status data to enrich posts
            for (final Post post : posts) {
                final Status status = statuses.get(post.getTwitterId());
                if (status != null) {
                    // Enrich post date and text
                    if (post.getDate() == null) {
                        post.setDate(status.getCreatedAt());
                    }
                    if (post.getText() == null) {
                        post.setText(status.getText());
                    }

                    // Enrich post author
                    final User user = status.getUser();
                    if (user != null) {
                        if (post.getAuthorUsername() == null) {
                            post.setAuthorUsername(user.getScreenName());
                        }
                        if (post.getAuthorFullName() == null) {
                            post.setAuthorFullName(user.getName());
                        }
                        if (post.getAuthorDescription() == null) {
                            post.setAuthorDescription(user.getDescription());
                        }
                    }

                    // Enrich mention / hashtag / url annotations
                    for (final UserMentionEntity e : status.getUserMentionEntities()) {
                        final MentionAnnotation m = post.addAnnotation(MentionAnnotation.class,
                                e.getStart(), e.getEnd());
                        if (m.getFullName() == null) {
                            m.setFullName(e.getName());
                        }
                    }
                    for (final HashtagEntity e : status.getHashtagEntities()) {
                        post.addAnnotation(HashtagAnnotation.class, e.getStart(), e.getEnd());
                    }
                    for (final URLEntity e : status.getURLEntities()) {
                        post.addAnnotation(UrlAnnotation.class, e.getStart(), e.getEnd());
                    }
                }
            }
        }

        private void enrichViaUserLookup(final Iterable<Post> posts) throws TwitterException {

            // Collect the usernames of the users that is possible & useful to gather from Twitter
            final Set<String> usernames = new HashSet<>();
            for (final Post post : posts) {
                if (post.getAuthorUsername() != null && (post.getAuthorFullName() == null
                        || post.getAuthorDescription() == null)) {
                    usernames.add(post.getAuthorUsername());
                }
                for (final MentionAnnotation m : post.getAnnotations(MentionAnnotation.class)) {
                    if (m.getFullName() == null || m.getDescription() == null) {
                        usernames.add(m.getUsername());
                    }
                }
            }

            // Gather statuses in batches of 100 (most efficient way)
            final Map<String, User> users = new HashMap<>();
            for (final List<String> batch : Iterables.partition(usernames, 100)) {
                for (final User user : this.twitter
                        .lookupUsers(batch.toArray(new String[batch.size()]))) {
                    users.put(user.getScreenName(), user);
                }
            }

            // Use retrieved user data to enrich posts
            for (final Post post : posts) {
                // Enrich the post author
                final User author = users.get(post.getAuthorUsername());
                if (author != null) {
                    if (post.getAuthorFullName() == null) {
                        post.setAuthorFullName(author.getName());
                    }
                    if (post.getAuthorDescription() == null) {
                        post.setAuthorDescription(author.getDescription());
                    }
                }

                // Enrich mention annotations
                for (final MentionAnnotation m : post.getAnnotations(MentionAnnotation.class)) {
                    final User user = users.get(m.getUsername());
                    if (user != null) {
                        if (m.getFullName() == null) {
                            m.setFullName(user.getName());
                        }
                        if (m.getDescription() == null) {
                            m.setDescription(user.getDescription());
                        }
                    }
                }
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + this.twitter + ")";
        }

    }

}
