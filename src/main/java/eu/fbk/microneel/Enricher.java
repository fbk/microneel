package eu.fbk.microneel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.microneel.util.TwitterBuilder;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.IO;
import twitter4j.HashtagEntity;
import twitter4j.Query;
import twitter4j.Query.ResultType;
import twitter4j.QueryResult;
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

    public static Enricher createTagdefEnricher(@Nullable final String lang) {
        return new TagdefEnricher(lang);
    }

    public static Enricher createUrlEnricher() {
        return UrlEnricher.INSTANCE;
    }

    public static Enricher create(final Path basePath, final Properties properties, String prefix) {

        // Normalize prefix, ensuring it ends with '.'
        prefix = Strings.isNullOrEmpty(prefix) ? "" : prefix.endsWith(".") ? prefix : prefix + ".";

        // Build a list of enrichers to be later combined
        final List<Enricher> enrichers = new ArrayList<>();

        // Retrieve the types of enrichers enabled in the configuration
        final Set<String> types = ImmutableSet
                .copyOf(properties.getProperty(prefix + "type", "").split("\\s+"));

        // Add Twitter API enricher, if configured
        if (types.contains("api")) {
            enrichers.add(createTwitterApiEnricher(
                    new TwitterBuilder().setProperties(properties, prefix + "api.").build()));
        }

        // Add Tagdef enricher, if configured
        if (types.contains("tagdef")) {
            final String lang = properties.getProperty(prefix + "tagdef.lang");
            enrichers.add(createTagdefEnricher(Strings.emptyToNull(lang)));
        }

        // Add a URL enricher, if configured
        if (types.contains("url")) {
            enrichers.add(createUrlEnricher());
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
            final Enricher enricher = Enricher.create(configFile.toPath().getParent(), config,
                    configPrefix);
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

    private static final class ConcatEnricher extends Enricher {

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

    private static final class NullEnricher extends Enricher {

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

    private static final class TwitterApiEnricher extends Enricher {

        private final Twitter twitter;

        TwitterApiEnricher(final Twitter twitter) {
            this.twitter = twitter;
        }

        @Override
        public void enrich(final Iterable<Post> posts) throws TwitterException {
            enrichViaStatusLookup(posts);
            enrichViaUserLookup(posts);
            enrichViaHashtagSearch(posts);
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

        // TODO: may also look for profiles matching the hashtag and use their usenames

        private void enrichViaHashtagSearch(final Iterable<Post> posts) throws TwitterException {

            // Collect the hashtags that is possible & useful to search in Twitter
            final Set<String> hashtags = new HashSet<>();
            for (final Post post : posts) {
                for (final HashtagAnnotation h : post.getAnnotations(HashtagAnnotation.class)) {
                    if (h.getTokenization() == null) {
                        hashtags.add(h.getHashtag().toLowerCase());
                    }
                }
            }

            // Initializa a structure holding candidate hashtag tokenizations and their counts
            final Map<String, Multiset<String>> candidateTokenizations = new HashMap<>();
            for (final String hashtag : hashtags) {
                candidateTokenizations.put(hashtag, HashMultiset.create());
            }

            // Fill the structure by sending a search request (100 tweets out) for each hashtag
            LOGGER.debug("Searching {} hashtags", hashtags.size());
            for (final String hashtag : hashtags) {
                final Query query = new Query("#" + hashtag);
                query.setCount(100);
                query.setResultType(ResultType.recent);
                final QueryResult result = this.twitter.search(query);
                LOGGER.debug("{} tweets for #{}", result.getTweets().size(), hashtag);
                for (final Status status : result.getTweets()) {
                    for (final HashtagEntity e : status.getHashtagEntities()) {
                        final Multiset<String> multiset = candidateTokenizations
                                .get(e.getText().toLowerCase());
                        if (multiset != null) {
                            final String tokenization = extractTokenization(e.getText());
                            LOGGER.debug("Tokenization for {}: {}", e.getText(), tokenization);
                            if (tokenization != null) {
                                multiset.add(tokenization);
                            }
                        }
                    }
                }
            }

            // Choose the best tokenization for each hashtag
            final Map<String, String> tokenizations = new HashMap<>();
            for (final Map.Entry<String, Multiset<String>> entry : candidateTokenizations
                    .entrySet()) {
                String bestTokenization = null;
                int bestCount = 0;
                for (final String tokenization : entry.getValue()) {
                    final int count = entry.getValue().count(tokenization);
                    if (count > bestCount || count == bestCount
                            && tokenization.length() > bestTokenization.length()) {
                        bestTokenization = tokenization;
                        bestCount = count;
                    }
                }
                if (bestCount > 0) {
                    tokenizations.put(entry.getKey(), bestTokenization);
                }
                LOGGER.debug("Tokenization for #{}: {} ({}/{} occurrences)", entry.getKey(),
                        bestTokenization, bestCount, entry.getValue().size());
            }

            // Enrich hashtag annotations with found tokenizations
            int numEnrichments = 0;
            for (final Post post : posts) {
                for (final HashtagAnnotation h : post.getAnnotations(HashtagAnnotation.class)) {
                    if (h.getTokenization() == null) {
                        h.setTokenization(tokenizations.get(h.getHashtag().toLowerCase()));
                        ++numEnrichments;
                    }
                }
            }
            LOGGER.debug("{} hashtags enriched in {} posts", numEnrichments, Iterables.size(posts));
        }

        @Nullable
        private static String extractTokenization(final String string) {
            if (string.equals(string.toLowerCase()) || string.equals(string.toUpperCase())) {
                return null;
            }
            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < string.length(); ++i) {
                if (i > 0) {
                    final char c1 = string.charAt(i - 1);
                    final char c2 = string.charAt(i);
                    if (Character.isUpperCase(c2) && !Character.isUpperCase(c1)
                            || Character.isDigit(c2) && !Character.isDigit(c1)
                            || !Character.isDigit(c2) && Character.isDigit(c2)) {
                        builder.append(' ');
                    }
                }
                builder.append(string.charAt(i));
            }
            return builder.toString();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + this.twitter + ")";
        }

    }

    private static final class TagdefEnricher extends Enricher {

        @Nullable
        private final String lang;

        TagdefEnricher(@Nullable final String lang) {
            this.lang = lang;
        }

        @Override
        public void enrich(final Iterable<Post> posts) throws Throwable {

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
                    LOGGER.warn("Ignoring exception while fetching definitions for #" + hashtag,
                            ex);
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

    }

    private static final class UrlEnricher extends Enricher {

        static final UrlEnricher INSTANCE = new UrlEnricher();

        private static final Pattern TITLE_PATTERN = Pattern.compile("\\<title>(.*)\\</title>",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        @Override
        public void enrich(final Iterable<Post> posts) throws Throwable {

            // Collect the URLs to dereference
            final Set<String> urls = new HashSet<>();
            for (final Post post : posts) {
                for (final UrlAnnotation a : post.getAnnotations(UrlAnnotation.class)) {
                    if (a.getResolvedUrl() == null || a.getTitle() == null) {
                        urls.add(a.getUrl());
                    }
                }
            }

            // Dereference URLs
            LOGGER.debug("Fetching {} urls", urls.size());
            final Map<String, String> resolvedUrls = new HashMap<>();
            final Map<String, String> titles = new HashMap<>();
            for (final String url : urls) {
                try {
                    LOGGER.debug("Fetching {}", url);
                    String currentUrl = url;
                    for (int i = 0; i < 5; ++i) {
                        final HttpURLConnection connection = (HttpURLConnection) new URL(currentUrl)
                                .openConnection();
                        connection.setRequestProperty("Accept", "text/html");
                        connection.connect();
                        final int code = connection.getResponseCode();
                        if (code == HttpURLConnection.HTTP_MOVED_TEMP
                                || code == HttpURLConnection.HTTP_MOVED_PERM
                                || code == HttpURLConnection.HTTP_SEE_OTHER) {
                            currentUrl = connection.getHeaderField("Location");
                        } else {
                            if (code >= 200 && code < 300) {
                                try {
                                    final String content = CharStreams.toString(
                                            new InputStreamReader(connection.getInputStream(),
                                                    Charsets.UTF_8)); // TODO: handle others
                                    final Matcher matcher = TITLE_PATTERN.matcher(content);
                                    if (matcher.find()) {
                                        String title = matcher.group(1);
                                        final int index = title.indexOf("<");
                                        title = index < 0 ? title : title.substring(0, index);
                                        title = StringEscapeUtils.unescapeHtml4(title).trim();
                                        title = title.replace('\n', ' ');
                                        titles.put(url, title);
                                        LOGGER.debug("Found title for {}: {}", url, title);
                                    }
                                    resolvedUrls.put(url, currentUrl);
                                } finally {
                                    IO.closeQuietly(connection.getInputStream());
                                }
                            }
                            break;
                        }
                    }
                } catch (final Throwable ex) {
                    LOGGER.error("Failed fetching URL " + url, ex);
                }
            }

            // Enrich URL annotations in posts
            for (final Post post : posts) {
                for (final UrlAnnotation a : post.getAnnotations(UrlAnnotation.class)) {
                    if (a.getResolvedUrl() == null) {
                        a.setResolvedUrl(resolvedUrls.get(a.getUrl()));
                    }
                    if (a.getTitle() == null) {
                        a.setTitle(titles.get(a.getUrl()));
                    }
                }
            }
        }

    }

}
