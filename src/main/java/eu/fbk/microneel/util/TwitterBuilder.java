package eu.fbk.microneel.util;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

// Interesting configuration methods:
// - setJSONStoreEnabled(boolean enabled) default false
// - setIncludeMyRetweetEnabled(boolean enabled) default true
// - setIncludeEntitiesEnabled(boolean enabled) default true
// - setTrimUserEnabled(boolean enabled) default false
// - setContributingTo(long contributingTo) default -1

public final class TwitterBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterBuilder.class);

    private static final long REQUEST_LIMIT_WINDOW_LENGTH = 15 * 60 * 1000 + 5000; // 5 s margin

    private static final long CONSECUTIVE_EXCEPTION_MIN_DELAY = 10 * 1000L; // 10 s

    private static final long CONSECUTIVE_EXCEPTION_MAX_DELAY = 60 * 60 * 1000L; // 1h

    private static final long CONSECUTIVE_EXCEPTION_MULTIPLIER = 6; // 10s 1m 6m 36m 1h

    private static final AtomicLong REQUEST_COUNTER = new AtomicLong(0);

    private int maxConsecutiveExceptions;

    private int userRequestLimit;

    private int appRequestLimit;

    private long avgRequestSeparation;

    private boolean mbeanEnabled;

    private String proxyHost;

    private int proxyPort;

    private String proxyUser;

    private String proxyPassword;

    private final Set<List<String>> appCredentials;

    private final Set<List<String>> userCredentials;

    public TwitterBuilder() {
        this.maxConsecutiveExceptions = 0;
        this.userRequestLimit = 0;
        this.appRequestLimit = 0;
        this.avgRequestSeparation = 0;
        this.mbeanEnabled = false;
        this.proxyHost = null;
        this.proxyPort = 0;
        this.proxyUser = null;
        this.proxyPassword = null;
        this.appCredentials = new HashSet<>();
        this.userCredentials = new HashSet<>();
    }

    public TwitterBuilder setProperties(final Properties properties, final String prefix) {

        // Retrieve maximum user and app request limits (further restricting Twitter limits)
        final String p = prefix == null ? "" : prefix.endsWith(".") ? prefix : prefix + ".";

        // Read limits, if supplied
        this.maxConsecutiveExceptions = Integer.parseInt(properties
                .getProperty(p + "maxConsecutiveExceptions", "" + this.maxConsecutiveExceptions));
        this.userRequestLimit = Integer.parseInt(properties //
                .getProperty(p + "userRequestLimit", "" + this.userRequestLimit));
        this.appRequestLimit = Integer.parseInt(properties //
                .getProperty(p + "appRequestLimit", "" + this.appRequestLimit));
        this.avgRequestSeparation = Long.parseLong(properties //
                .getProperty(p + "avgRequestSeparation", "" + this.avgRequestSeparation));

        // Read MBean server setting
        this.mbeanEnabled = Boolean.parseBoolean( //
                properties.getProperty(p + "mbeanEnabled", "" + this.mbeanEnabled));

        // Read proxy settings, if supplied
        this.proxyHost = properties.getProperty(p + "proxyHost", this.proxyHost);
        this.proxyPort = Integer.parseInt( //
                properties.getProperty(p + "proxyPort", "" + this.proxyPort));
        this.proxyUser = properties.getProperty(p + "proxyUser", this.proxyUser);
        this.proxyPassword = properties.getProperty(p + "proxyPassword", this.proxyPassword);

        // Read supplied account settings
        for (int i = -1; i <= properties.size() / 4; ++i) {
            final String s = i == -1 ? "" : Integer.toString(i);
            final String ck = properties.getProperty(p + "consumerKey" + s);
            final String cs = properties.getProperty(p + "consumerSecret" + s);
            final String at = properties.getProperty(p + "accessToken" + s);
            final String ats = properties.getProperty(p + "accessTokenSecret" + s);
            if (ck != null && cs != null && at != null && ats != null) {
                this.userCredentials.add(ImmutableList.of(ck, cs, at, ats));
            } else if (ck != null && cs != null && at == null && ats == null) {
                this.appCredentials.add(ImmutableList.of(ck, cs));
            } else if (ck != null || cs != null || at != null || ats != null) {
                LOGGER.warn("Incomplete credentials for " + p + "consumerKey" + s + "... ");
            }
        }
        return this;
    }

    public TwitterBuilder setMaxConsecutiveExceptions(final int maxConsecutiveExceptions) {
        this.maxConsecutiveExceptions = maxConsecutiveExceptions;
        return this;
    }

    public TwitterBuilder setUserRequestLimit(final int userRequestLimit) {
        this.userRequestLimit = userRequestLimit;
        return this;
    }

    public TwitterBuilder setAppRequestLimit(final int appRequestLimit) {
        this.appRequestLimit = appRequestLimit;
        return this;
    }

    public TwitterBuilder setAvgRequestSeparation(final long avgRequestSeparation) {
        this.avgRequestSeparation = avgRequestSeparation;
        return this;
    }

    public TwitterBuilder setMBeanEnabled(final boolean mbeanEnabled) {
        this.mbeanEnabled = mbeanEnabled;
        return this;
    }

    public TwitterBuilder setProxy(@Nullable final String proxyHost, final int proxyPort,
            @Nullable final String proxyUser, @Nullable final String proxyPassword) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUser = proxyUser;
        this.proxyPassword = proxyPassword;
        return this;
    }

    public TwitterBuilder addAppAccount(final String consumerKey, final String consumerSecret) {
        this.appCredentials.add(ImmutableList.of( //
                Objects.requireNonNull(consumerKey), Objects.requireNonNull(consumerSecret)));
        return this;
    }

    public TwitterBuilder addUserAccount(final String consumerKey, final String consumerSecret,
            final String accessToken, final String accessTokenSecret) {
        this.userCredentials.add(ImmutableList.of( //
                Objects.requireNonNull(consumerKey), Objects.requireNonNull(consumerSecret),
                Objects.requireNonNull(accessToken), Objects.requireNonNull(accessTokenSecret)));
        return this;
    }

    public Twitter build() {

        // Allocate a list to populate with the Twitter objects for the various credentials supplied
        final List<Twitter> wrappedTwitters = new ArrayList<>();

        // Create Twitter objects for application authentication
        for (final List<String> credentials : this.appCredentials) {
            try {
                final ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setJSONStoreEnabled(false);
                cb.setGZIPEnabled(true);
                cb.setOAuthConsumerKey(credentials.get(0));
                cb.setOAuthConsumerSecret(credentials.get(1));
                cb.setApplicationOnlyAuthEnabled(true);
                final ConfigurationBuilder cbToken = new ConfigurationBuilder();
                cbToken.setApplicationOnlyAuthEnabled(true);
                cbToken.setOAuthConsumerKey(credentials.get(0));
                cbToken.setOAuthConsumerSecret(credentials.get(1));
                final OAuth2Token token;
                try {
                    token = new TwitterFactory(cbToken.build()).getInstance().getOAuth2Token();
                } catch (final TwitterException ex) {
                    throw Throwables.propagate(ex);
                }
                cb.setOAuth2TokenType(token.getTokenType());
                cb.setOAuth2AccessToken(token.getAccessToken());
                final Twitter appTwitter = new TwitterFactory(cb.build()).getInstance();
                appTwitter.getAPIConfiguration(); // validate credentials
                wrappedTwitters.add(appTwitter);
                LOGGER.info("Using {}", describe(appTwitter));
            } catch (final Throwable ex) {
                LOGGER.error("Cannot configure " + describe(credentials) + " - skipping", ex);
            }
        }

        // Create Twitter objects for user authentication
        for (final List<String> credentials : this.userCredentials) {
            try {
                final ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setJSONStoreEnabled(false);
                cb.setGZIPEnabled(true);
                cb.setOAuthConsumerKey(credentials.get(0));
                cb.setOAuthConsumerSecret(credentials.get(1));
                cb.setOAuthAccessToken(credentials.get(2));
                cb.setOAuthAccessTokenSecret(credentials.get(3));
                cb.setApplicationOnlyAuthEnabled(false);
                final Twitter userTwitter = new TwitterFactory(cb.build()).getInstance();
                final String screenName = userTwitter.getScreenName();
                wrappedTwitters.add(userTwitter);
                LOGGER.info("Using {} ({})", describe(userTwitter), screenName);
            } catch (final Throwable ex) {
                LOGGER.error("Cannot configure " + describe(credentials) + " - skipping", ex);
            }
        }

        // Wrap the Twitter objects in a single dynamic proxy implementing rate limiting
        final Handler handler = new Handler(ImmutableList.copyOf(wrappedTwitters),
                Math.max(0L, this.avgRequestSeparation),
                this.maxConsecutiveExceptions > 0 ? this.maxConsecutiveExceptions
                        : Integer.MAX_VALUE,
                this.userRequestLimit > 0 ? this.userRequestLimit : Integer.MAX_VALUE,
                this.appRequestLimit > 0 ? this.appRequestLimit : Integer.MAX_VALUE);
        return (Twitter) Proxy.newProxyInstance(Handler.class.getClassLoader(),
                new Class<?>[] { Twitter.class }, handler);
    }

    private static String describe(final Twitter twitter) {
        final Configuration c = twitter.getConfiguration();
        return describe(c.isApplicationOnlyAuthEnabled()
                ? ImmutableList.of(c.getOAuthConsumerKey(), c.getOAuthConsumerSecret())
                : ImmutableList.of(c.getOAuthConsumerKey(), c.getOAuthConsumerSecret(),
                        c.getOAuthAccessToken(), c.getOAuthAccessTokenSecret()));
    }

    private static String describe(final List<String> credentials) {
        if (credentials.size() == 2) {
            return "app auth (ck=" + credentials.get(0).substring(0, 3) + ".. cs="
                    + credentials.get(1).substring(0, 3) + "..)";
        } else {
            final String at = credentials.get(2);
            return "user auth (ck=" + credentials.get(0).substring(0, 3) + ".. cs="
                    + credentials.get(1).substring(0, 3) + ".. uid="
                    + at.substring(0, at.indexOf("-")) + ")";
        }
    }

    private static final class Handler implements InvocationHandler {

        private final Map<String, Bucket> buckets;

        private final int maxConsecutiveExceptions;

        private long consecutiveExceptionsDelay;

        private int consecutiveExceptions;

        private Handler(final List<Twitter> wrappedTwitters, final long avgRequestSeparation,
                final int maxConsecutiveExceptions, final int userRequestLimit,
                final int appRequestLimit) {

            this.maxConsecutiveExceptions = maxConsecutiveExceptions;
            this.consecutiveExceptionsDelay = CONSECUTIVE_EXCEPTION_MIN_DELAY;
            this.consecutiveExceptions = 0;
            this.buckets = new HashMap<>();

            // Initialize buckets, based on configuration in TwitterRateLimiter.tsv
            try {
                int defaultUserRequestLimit = 15;
                int defaultAppRequestLimit = 15;
                for (final String line : Resources.readLines(
                        TwitterBuilder.class.getResource("TwitterBuilder.tsv"), Charsets.UTF_8)) {
                    final String[] tokens = line.split("\t");
                    final String name = tokens[0].trim();
                    final int userLimit = Math.min(userRequestLimit,
                            Integer.parseInt(tokens[2].trim()));
                    final int appLimit = Math.min(appRequestLimit,
                            Integer.parseInt(tokens[3].trim()));
                    if (name.equals("*")) {
                        defaultUserRequestLimit = userLimit;
                        defaultAppRequestLimit = appLimit;
                    } else {
                        final String[] methods = tokens[1].trim().split(",");
                        final Bucket bucket = new Bucket(name, wrappedTwitters, userLimit, appLimit,
                                avgRequestSeparation);
                        for (final String method : methods) {
                            this.buckets.put(method, bucket);
                        }
                    }
                }
                for (final Method method : Twitter.class.getMethods()) {
                    if (!this.buckets.containsKey(method.getName())) {
                        this.buckets.put(method.getName(),
                                new Bucket(method.getName(), wrappedTwitters,
                                        defaultUserRequestLimit, defaultAppRequestLimit,
                                        avgRequestSeparation));
                    }
                }
            } catch (final IOException ex) {
                throw new Error(ex);
            }
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args)
                throws Throwable {

            // Retrieve bucket for current request
            final Bucket bucket = this.buckets.get(method.getName());

            // Take timestamp and log beginning of request
            final long ts = System.currentTimeMillis();
            final long requestNum = REQUEST_COUNTER.incrementAndGet();
            if (LOGGER.isDebugEnabled()) {
                final String argsStr = Arrays.deepToString(args);
                final StringBuilder builder = new StringBuilder();
                builder.append("[Twitter #").append(requestNum);
                builder.append(" ").append(method.getName()).append("]");
                builder.append(" call, ").append(bucket).append(" --> ");
                builder.append(argsStr.substring(1, argsStr.length() - 1));
                LOGGER.debug(builder.toString());
            }

            // Perform the request, possibly retrying it multiple times
            Object result = null;
            final long[] durationHolder = new long[1];
            try {
                result = invokeHelper(proxy, method, args, bucket, requestNum, durationHolder);

            } catch (final Throwable ex) {
                result = ex;

            } finally {
                // Log request completion
                if (LOGGER.isDebugEnabled()) {
                    final StringBuilder builder = new StringBuilder();
                    builder.append("[Twitter #").append(requestNum);
                    builder.append(" ").append(method.getName()).append("]");
                    builder.append(result instanceof Throwable ? " fail, " : " done, ");
                    builder.append(durationHolder[0]).append(" ms (");
                    builder.append(System.currentTimeMillis() - ts).append(" incl. wait) <-- ");
                    builder.append(result == null ? "null" : result.getClass().getSimpleName());
                    if (result instanceof Collection) {
                        builder.append(" (").append(((Collection<?>) result).size())
                                .append(" items)");
                    }
                    LOGGER.debug(builder.toString());
                }
            }

            // Either return the result or propagate the exception
            if (result instanceof Throwable) {
                throw (Throwable) result;
            } else {
                return result;
            }
        }

        private Object invokeHelper(final Object proxy, final Method method, final Object[] args,
                final Bucket bucket, final long requestNum, final long[] durationHolder)
                throws Throwable {

            // Select the Twitter object to delegate this request to. Wait if necessary
            Twitter twitter;
            try {
                twitter = bucket.get();

            } catch (final InterruptedException ex) {
                // Mark thread as interrupted and propagate exception
                Thread.currentThread().interrupt();
                throw new RuntimeException("Operation interrupted", ex);

            } catch (final Throwable ex) {
                // Should not happen. Propagate an error
                throw new Error("Exception caught while looking for suitable twitter object "
                        + "to handle " + method.getName() + " request", ex);
            }

            // Perform the request
            final long ts = System.currentTimeMillis();
            try {
                final Object result = method.invoke(twitter, args);
                durationHolder[0] = System.currentTimeMillis() - ts;
                synchronized (this) {
                    this.consecutiveExceptions = 0;
                    this.consecutiveExceptionsDelay = CONSECUTIVE_EXCEPTION_MIN_DELAY;
                }
                return result;

            } catch (final InvocationTargetException ex) {

                // Update the number of consecutive exceptions. Force a delay if threshold reached
                synchronized (this) {
                    ++this.consecutiveExceptions;
                    if (this.consecutiveExceptions >= this.maxConsecutiveExceptions) {
                        LOGGER.warn("[Twitter #" + requestNum + " " + method.getName() + "] "
                                + this.consecutiveExceptions
                                + " consecutive exceptions detected - forcing a delay of "
                                + this.consecutiveExceptionsDelay + " ms (using "
                                + describe(twitter) + ")");
                        for (final Bucket b : this.buckets.values()) {
                            b.shift(this.consecutiveExceptionsDelay);
                        }
                        this.consecutiveExceptions = 0; // reset counter
                        this.consecutiveExceptionsDelay = Math.min(CONSECUTIVE_EXCEPTION_MAX_DELAY,
                                this.consecutiveExceptionsDelay * CONSECUTIVE_EXCEPTION_MULTIPLIER);
                    }
                }

                // Either retry the operation, or propagate the exception
                final Throwable wrappedEx = ex.getCause();
                if (wrappedEx instanceof TwitterException) {
                    final TwitterException tex = (TwitterException) wrappedEx;
                    if (tex.getErrorCode() == 88) {
                        final int invalidatedTokens = bucket.invalidate(twitter);
                        LOGGER.info("[Twitter #" + requestNum + " " + method.getName()
                                + "] Rate limit exceeded, rescheduled " + invalidatedTokens
                                + " requests and retrying");
                        return invokeHelper(proxy, method, args, bucket, requestNum,
                                durationHolder);
                    } else if (tex.getStatusCode() == 403 && tex.getErrorCode() == 326) {
                        final int invalidatedTokens = bucket.invalidate(twitter);
                        LOGGER.warn("[Twitter #" + requestNum + " " + method.getName()
                                + "] Got banned using " + describe(twitter) + " with args "
                                + Arrays.deepToString(args) + ", invalidatated " + invalidatedTokens
                                + " requests and retrying");
                        return invokeHelper(proxy, method, args, bucket, requestNum,
                                durationHolder);
                    } else if (tex.getStatusCode() == 503) {
                        LOGGER.info("[Twitter #" + requestNum + " "
                                + method.getName() + "] " + (tex.getErrorCode() == 130
                                        ? "server overloaded" : "service unavailable")
                                + ": wait 1s and retry");
                        Thread.sleep(1000);
                        return invokeHelper(proxy, method, args, bucket, requestNum,
                                durationHolder);
                    }
                }
                durationHolder[0] = System.currentTimeMillis() - ts;

                // Restore interrupt flag if possible (Twitter4j eats InterruptedExceptions)
                for (Throwable e = wrappedEx; e != null; e = e.getCause()) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }

                // Propagate
                throw wrappedEx != null ? wrappedEx : ex;
            }
        }

    }

    private static final class Bucket {

        private final long avgRequestSeparation;

        private final DelayQueue<Token> queue;

        private long ts;

        Bucket(final String name, final Iterable<Twitter> twitters, final int userRequestLimit,
                final int appRequestLimit, final long avgRequestSeparation) {

            // Store parameters and initialize empty queue
            this.avgRequestSeparation = Math.max(0, avgRequestSeparation);
            this.queue = new DelayQueue<>();
            this.ts = System.currentTimeMillis();

            // Fill the queue with a token for each allowed request, considering all Twitter
            // objects
            for (final Twitter twitter : twitters) {
                final boolean isApp = twitter.getConfiguration().isApplicationOnlyAuthEnabled();
                final int requestLimit = isApp ? appRequestLimit : userRequestLimit;
                for (int i = 0; i < requestLimit; ++i) {
                    this.queue.offer(new Token(twitter, this.ts, true));
                    this.ts += getRandomWaitTime();
                }
            }

            // Log bucket characteristics
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} bucket: {} requests, {} connections total", name,
                        this.queue.size(), Iterables.size(twitters));
            }
        }

        Twitter get() throws InterruptedException {

            // Retrieve the head token, waiting for it becoming available, if necessary
            final Token token = this.queue.take();

            // Put back a new token in the queue, with ts = now + 15 minutes
            synchronized (this) {
                final long now = System.currentTimeMillis();
                this.ts = Math.max(this.ts + getRandomWaitTime(),
                        now + REQUEST_LIMIT_WINDOW_LENGTH);
                this.queue.offer(new Token(token.twitter, this.ts, false));
            }

            // Return the twitter object associated to the retrieved token
            return token.twitter;
        }

        synchronized void shift(final long delay) {
            final List<Token> oldTokens = Lists.newArrayList(this.queue);
            this.queue.clear();
            for (final Token oldToken : oldTokens) {
                this.queue.offer(new Token(oldToken.twitter, //
                        oldToken.ts + delay, oldToken.assumed));
            }
        }

        synchronized int invalidate(final Twitter twitter) {

            // Replace tokens that may correspond to illegal calls with new tokens
            // rescheduled after 15 minutes. Replaced tokens are the ones for which:
            // - corresponding Twitter object is the one being invalidated, AND
            // - either their ts is in the past (so another process can have consumed the call)
            // - or they are 'assumed', i.e., we don't know for sure whether the call is allowed
            int invalidatedTokens = 0;
            final long now = System.currentTimeMillis();
            for (final Token token : Lists.newArrayList(this.queue)) {
                if (twitter == token.twitter && (token.ts <= now || token.assumed)) {
                    this.queue.remove(token);
                    this.ts = Math.max(this.ts + getRandomWaitTime(),
                            now + REQUEST_LIMIT_WINDOW_LENGTH);
                    this.queue.offer(new Token(token.twitter, this.ts, false));
                    ++invalidatedTokens;
                }
            }
            return invalidatedTokens;
        }

        @Override
        public String toString() {

            // Take current timestamp
            final long now = System.currentTimeMillis();

            // Compute, holding a lock, (1) #tokens, #available tokens, time to wait to
            long ts = 0L;
            int size, available = 0;
            synchronized (this) {
                size = this.queue.size();
                for (final Token t : this.queue) {
                    if (t.ts > now) {
                        ts = t.ts;
                        break;
                    }
                    ++available;
                }
            }

            // Compose and return the result string
            return available + "/" + size + " req. available"
                    + (available > 0 ? "" : ", wait " + (ts - now) + " ms");
        }

        private long getRandomWaitTime() {
            if (this.avgRequestSeparation >= 0L) {
                final double random = Math.random();
                if (random > 0.0) { // should always be the case
                    return (long) (-this.avgRequestSeparation * Math.log(1.0 - random));
                }
            }
            return this.avgRequestSeparation;
        }

    }

    private static final class Token implements Delayed {

        final Twitter twitter;

        final long ts;

        final boolean assumed; // true if we assume we can do a call at that time

        Token(final Twitter twitter, final long ts, final boolean assumed) {
            this.twitter = twitter;
            this.ts = ts;
            this.assumed = assumed;
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(this.ts - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(final Delayed object) {
            final Token other = (Token) object;
            int result = Long.compare(this.ts, other.ts);
            if (result == 0) {
                result = System.identityHashCode(this.twitter)
                        - System.identityHashCode(((Token) object).twitter);
            }
            return result;
        }

    }

}
