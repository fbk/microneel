package eu.fbk.microneel;

import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.utils.core.CommandLine;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String... args) {
        try {
            // Parse command line
            final CommandLine cmd = CommandLine.parser().withName("microneel")
                    .withHeader("Performs NERC, EL, and coreference resolution on microposts.")
                    .withOption("i", "input",
                            "specifies the input FILE with the enriched posts to process", "FILE",
                            CommandLine.Type.FILE_EXISTING, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read options
            final Path inputPath = cmd.getOptionValue("i", Path.class);

            // Read posts
            final List<Post> posts = Post.read(inputPath);
            LOGGER.info("Read {} posts from {}", posts.size(), inputPath);
            for (int i = 0; i < posts.size(); ++i) {
                LOGGER.info("Post #{}:  {}", i, posts.get(i));
            }

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }
    }

}
