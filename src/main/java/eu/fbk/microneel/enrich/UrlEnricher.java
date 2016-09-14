package eu.fbk.microneel.enrich;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.Post.UrlAnnotation;
import eu.fbk.utils.core.IO;

public final class UrlEnricher implements Annotator {

    public static final UrlEnricher INSTANCE = new UrlEnricher();

    private static final Logger LOGGER = LoggerFactory.getLogger(UrlEnricher.class);

    private static final Pattern TITLE_PATTERN = Pattern.compile("\\<title>(.*)\\</title>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private UrlEnricher() {
    }

    @Override
    public void annotate(final Iterable<Post> posts) throws Throwable {

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
                                final String content = CharStreams.toString(new InputStreamReader(
                                        connection.getInputStream(), Charsets.UTF_8)); // TODO
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

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
