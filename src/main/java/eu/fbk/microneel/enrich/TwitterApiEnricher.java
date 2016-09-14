package eu.fbk.microneel.enrich;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Longs;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.HashtagAnnotation;
import eu.fbk.microneel.Post.MentionAnnotation;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.microneel.util.TwitterBuilder;
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

public final class TwitterApiEnricher implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterApiEnricher.class);

    private final Twitter twitter;

    public TwitterApiEnricher(final JsonObject json) {
        this.twitter = new TwitterBuilder().setProperties(json.getAsJsonObject("connection"))
                .build();
    }

    public TwitterApiEnricher(final Twitter twitter) {
        this.twitter = Objects.requireNonNull(twitter);
    }

    @Override
    public void annotate(final Iterable<Post> posts) throws TwitterException {
        enrichViaStatusLookup(posts);
        enrichViaUserLookup(posts);
        enrichViaHashtagSearch(posts);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.twitter + ")";
    }

    private void enrichViaStatusLookup(final Iterable<Post> posts) throws TwitterException {

        // Collect the IDs of the statuses that is possible & useful to gather from Twitter
        final Set<Long> ids = new HashSet<>();
        for (final Post post : posts) {
            final Long id = post.getTwitterId();
            if (id != null && (post.getDate() == null || post.getText() == null
                    || post.getLang() == null || post.getAuthorUsername() == null
                    || post.getAuthorFullName() == null || post.getAuthorDescription() == null
                    || post.getAuthorLang() == null || post.getAnnotations(MentionAnnotation.class)
                            .stream().anyMatch(a -> a.getFullName() == null))) {
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
                if (post.getLang() == null) {
                    final String lang = status.getLang();
                    post.setLang(lang == null ? null : lang.toLowerCase());
                }

                // Enrich post author
                final User user = status.getUser();
                if (user != null) {
                    if (post.getAuthorUsername() != null
                            && !post.getAuthorUsername().equalsIgnoreCase(user.getScreenName())) {
                        LOGGER.warn("Post author does not match author from Twitter:\npost:    "
                                + post.getAuthorUsername() + "\ntwitter: " + user.getScreenName());
                    } else {
                        if (post.getAuthorUsername() == null) {
                            post.setAuthorUsername(user.getScreenName());
                        }
                        if (post.getAuthorFullName() == null) {
                            post.setAuthorFullName(user.getName());
                        }
                        if (post.getAuthorDescription() == null) {
                            post.setAuthorDescription(user.getDescription());
                        }
                        if (post.getAuthorLang() == null) {
                            final String lang = user.getLang();
                            post.setAuthorLang(lang == null ? null : lang.toLowerCase());
                        }
                    }
                }

                // Enrich mention / hashtag / url annotations if text corresponds
                if (post.getText() != null && !post.getText().equals(status.getText())) {
                    LOGGER.warn("Post text does not match text from Twitter:\npost:    "
                            + post.getText() + "\ntwitter: " + status.getText());
                } else {
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
    }

    private void enrichViaUserLookup(final Iterable<Post> posts) throws TwitterException {

        // Collect the usernames of the users that is possible & useful to gather from Twitter
        final Set<String> usernames = new HashSet<>();
        for (final Post post : posts) {
            if (post.getAuthorUsername() != null && (post.getAuthorFullName() == null
                    || post.getAuthorDescription() == null || post.getAuthorLang() == null)) {
                usernames.add(post.getAuthorUsername());
            }
            for (final MentionAnnotation m : post.getAnnotations(MentionAnnotation.class)) {
                if (m.getFullName() == null || m.getDescription() == null || m.getLang() == null) {
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
                if (post.getAuthorLang() == null) {
                    final String lang = author.getLang();
                    post.setAuthorLang(lang == null ? null : lang.toLowerCase());
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
                    if (m.getLang() == null) {
                        final String lang = user.getLang();
                        m.setLang(lang == null ? null : lang.toLowerCase());
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
            try {
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
            } catch (final Throwable ex) {
                LOGGER.error("Search failed for hashtag " + hashtag, ex);
            }
        }

        // Choose the best tokenization for each hashtag
        final Map<String, String> tokenizations = new HashMap<>();
        for (final Map.Entry<String, Multiset<String>> entry : candidateTokenizations.entrySet()) {
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

}
