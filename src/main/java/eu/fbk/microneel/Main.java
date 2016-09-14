package eu.fbk.microneel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import eu.fbk.utils.core.CommandLine;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String... args) {
        try {
            // Parse command line
            final long ts = System.currentTimeMillis();
            final CommandLine cmd = CommandLine.parser().withName("microneel")
                    .withHeader("Performs NERC, EL, and coreference resolution on microposts.")
                    .withOption("c", "config",
                            "specifies the configuration file (default: microneel.json)", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, false)
                    .withOption("e", "enrich", "enriches input posts")
                    .withOption("r", "rewrite", "rewrites enriched posts")
                    .withOption("l", "link", "performs NERC and NEL on rewritten posts")
                    .withOption("s", "score", "scores linking results")
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read config
            final Path configPath = cmd.getOptionValue("c", Path.class,
                    Paths.get("microneel.json"));
            final JsonObject config = new GsonBuilder().create().fromJson(
                    com.google.common.io.Files.toString(configPath.toFile(), Charsets.UTF_8),
                    JsonObject.class);

            // Extract paths from config
            final JsonObject paths = config.getAsJsonObject("paths");
            final Path basePath = configPath.getParent();
            final Path postsInPath = basePath.resolve(paths.get("postsIn").getAsString());
            final Path postsEnrichedPath = basePath
                    .resolve(paths.get("postsEnriched").getAsString());
            final Path postsRewrittenPath = basePath
                    .resolve(paths.get("postsRewritten").getAsString());
            final Path postsLinkedPath = basePath.resolve(paths.get("postsLinked").getAsString());
            final Path goldStandardPath = basePath.resolve(paths.get("goldStandard").getAsString());

            // Build enricher
            final Annotator enricher = Annotator.create(config.getAsJsonObject("enricher"));
            LOGGER.info("Configured enricher: {}", enricher);

            // Build rewriter
            final Annotator rewriter = Annotator.create(config.getAsJsonObject("rewriter"));
            LOGGER.info("Configured rewriter: {}", rewriter);

            // Build linker
            final Annotator linker = Annotator.create(config.getAsJsonObject("linker"));
            LOGGER.info("Configured linker: {}", linker);

            // Determine commands to execute
            final boolean score = cmd.hasOption("s");
            final boolean link = cmd.hasOption("l") || score && !Files.exists(postsLinkedPath);
            final boolean rewrite = cmd.hasOption("r") || link && !Files.exists(postsRewrittenPath);
            final boolean enrich = cmd.hasOption("e")
                    || rewrite && !Files.exists(postsEnrichedPath);

            // Perform requested action(s)
            List<Post> posts = null;
            if (enrich) {
                posts = Post.read(postsInPath);
                LOGGER.info("Enriching {} posts", posts.size());
                enricher.annotate(posts);
                Post.write(postsEnrichedPath, posts);
            }
            if (rewrite) {
                posts = posts != null ? posts : Post.read(postsEnrichedPath);
                LOGGER.info("Rewriting {} posts", posts.size());
                rewriter.annotate(posts);
                Post.write(postsRewrittenPath, posts);
            }
            if (link) {
                posts = posts != null ? posts : Post.read(postsRewrittenPath);
                LOGGER.info("Linking {} posts", posts.size());
                linker.annotate(posts);
                Post.write(postsLinkedPath, posts);
            }
            if (score) {
                LOGGER.info("Scoring {} posts", posts.size());
                // TODO
            }

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }
    }

}
