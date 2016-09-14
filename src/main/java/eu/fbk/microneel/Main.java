package eu.fbk.microneel;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.util.CoreMap;
import eu.fbk.dh.tint.runner.TintPipeline;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String... args) {
        try {
//            // Parse command line
//            final CommandLine cmd = CommandLine.parser().withName("microneel")
//                    .withHeader("Performs NERC, EL, and coreference resolution on microposts.")
//                    .withOption("i", "input",
//                            "specifies the input FILE with the enriched posts to process", "FILE",
//                            CommandLine.Type.FILE_EXISTING, true, false, true)
//                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);
//
//            // Read options
//            final Path inputPath = cmd.getOptionValue("i", Path.class);
//
//            // Read posts
//            final List<Post> posts = Post.read(inputPath);
//            LOGGER.info("Read {} posts from {}", posts.size(), inputPath);
//            for (int i = 0; i < posts.size(); ++i) {
//                LOGGER.info("Post #{}:  {}", i, posts.get(i));
//            }

//            InputStream configStream = Main.class.getClassLoader().getResourceAsStream("/tint.properties");
            InputStream configStream = new FileInputStream(
                    "/Users/alessio/Documents/scripts/microneel/src/main/resources/tint.properties");

            TintPipeline pipeline = new TintPipeline();
            pipeline.loadPropertiesFromStream(configStream);
            pipeline.loadDefaultProperties();
            pipeline.load();

            Annotation annotation = pipeline.runRaw("I topi non avevano nipoti");

            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    System.out.println(token);
                }

            }

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }
    }

}
