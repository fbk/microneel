package eu.fbk.microneel;

import java.io.IOException;
import java.io.Writer;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.CharStreams;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import eu.fbk.microneel.Post.EntityAnnotation;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.core.IO;

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
            final Path annotationsOut = basePath.resolve(paths.get("annotationsOut").getAsString());
            final Path annotationsGold = basePath
                    .resolve(paths.get("annotationsGold").getAsString());

            // Determine commands to execute
            final boolean score = cmd.hasOption("s");
            final boolean link = cmd.hasOption("l") || score && !Files.exists(postsLinkedPath);
            final boolean rewrite = cmd.hasOption("r") || link && !Files.exists(postsRewrittenPath);
            final boolean enrich = cmd.hasOption("e")
                    || rewrite && !Files.exists(postsEnrichedPath);

            // Perform requested action(s)
            List<Post> posts = null;
            if (enrich) {
                final Annotator enricher = Annotator.create(config.getAsJsonObject("enricher"),
                        basePath);
                LOGGER.info("Configured enricher: {}", enricher);
                posts = Post.read(postsInPath);
                LOGGER.info("Enriching {} posts", posts.size());
                enricher.annotate(posts);
                Post.write(postsEnrichedPath, posts);
            }
            if (rewrite) {
                final Annotator rewriter = Annotator.create(config.getAsJsonObject("rewriter"),
                        basePath);
                LOGGER.info("Configured rewriter: {}", rewriter);
                posts = posts != null ? posts : Post.read(postsEnrichedPath);
                LOGGER.info("Rewriting {} posts", posts.size());
                rewriter.annotate(posts);
                Post.write(postsRewrittenPath, posts);
            }
            if (link) {
                final Annotator linker = Annotator.create(config.getAsJsonObject("linker"),
                        basePath);
                LOGGER.info("Configured linker: {}", linker);
                posts = posts != null ? posts : Post.read(postsRewrittenPath);
                LOGGER.info("Linking {} posts", posts.size());
                linker.annotate(posts);
                Post.write(postsLinkedPath, posts);
                writeResults(annotationsOut, posts);
            }
            if (score) {
                final Path tacAnnotationsOut = toTacFormat(annotationsOut, null);
                final Path tacAnnotationsGold = toTacFormat(annotationsGold, null);
                Process process = null;
                process = new ProcessBuilder("python", "-m", "neleval", "evaluate", "-g",
                        tacAnnotationsGold.toString(), tacAnnotationsOut.toString())
                                .directory(basePath.toFile()).redirectError(Redirect.INHERIT)
                                .start();
                try {
                    double stmm = 0.0;
                    double slm = 0.0;
                    double ceaf = 0.0;
                    final List<String> output = CharStreams
                            .readLines(IO.utf8Reader(process.getInputStream()));
                    if (output.isEmpty()) {
                        LOGGER.error("Counl not invoke 'neleval' TAC KBP evaluator "
                                + "(https://github.com/wikilinks/neleval). "
                                + "You may install it via 'https://github.com/wikilinks/neleval' "
                                + "(python, python-pip, python-scipy packages required)");
                    } else {
                        for (final String line : output) {
                            if (line.contains("strong_typed_mention_match")) {
                                stmm = Double.parseDouble(line.split("\t")[6]);
                            } else if (line.contains("strong_link_match")) {
                                slm = Double.parseDouble(line.split("\t")[6]);
                            } else if (line.contains("mention_ceaf")) {
                                ceaf = Double.parseDouble(line.split("\t")[6]);
                            }
                        }
                        final double s = 0.4 * ceaf + 0.3 * stmm + 0.3 * slm;
                        LOGGER.info("Score {} (0.4 ceaf + 0.3 stmm + 0.3 slm):\n{}", s,
                                Joiner.on("\n").join(output));
                    }
                } finally {
                    process.destroy();
                }
            }

            // Log completion
            LOGGER.info("Completed in {} ms", System.currentTimeMillis() - ts);

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }
    }

    private static void writeResults(final Path path, final Iterable<Post> posts)
            throws IOException {

        // Allocate a counter and a map for handling NIL labels
        final Map<String, Integer> nilMap = Maps.newHashMap();
        int nilCounter = 0;

        // Write a TSV where each line is an entity annotation: Tweet ID, begin, end, uri, category
        try (Writer writer = IO.utf8Writer(IO.buffer(IO.write(path.toString())))) {
            for (final Post post : posts) {
                for (final EntityAnnotation a : Ordering.natural()
                        .sortedCopy(post.getAnnotations(EntityAnnotation.class))) {
                    writer.write(Long.toString(post.getTwitterId()));
                    writer.write('\t');
                    writer.write(Integer.toString(a.getBeginIndex()));
                    writer.write('\t');
                    writer.write(Integer.toString(a.getEndIndex()));
                    writer.write('\t');
                    if (a.getUri() == null) {
                        writer.write("NIL");
                        writer.write(Integer.toString(++nilCounter));
                    } else if (a.getUri().startsWith("http://dbpedia.org/")) {
                        writer.write(a.getUri());
                    } else {
                        Integer index = nilMap.get(a.getUri());
                        if (index == null) {
                            index = ++nilCounter;
                            nilMap.put(a.getUri(), index);
                        }
                        writer.write("NIL");
                        writer.write(index.toString());
                    }
                    writer.write('\t');
                    writer.write(StringUtils.capitalize(a.getCategory().toString().toLowerCase()));
                    writer.write('\n');
                }
            }
        }
    }

    private static Path toTacFormat(final Path neelPath, @Nullable Path tacPath)
            throws IOException {

        // Create a temporary file for output if no TAC file path was supplied
        if (tacPath == null) {
            tacPath = Files.createTempFile("microneel", ".tsv");
            tacPath.toFile().deleteOnExit();
        }

        // Add an additional column filled with 1.0 after the first 4 columns
        try (final Writer writer = IO.utf8Writer(IO.buffer(IO.write(tacPath.toString())))) {
            for (final String line : Files.readAllLines(neelPath, Charsets.UTF_8)) {
                final String[] fields = line.split("\t");
                writer.write(String.format("%s\t%s\t%s\t%s\t1.0\t%s\n", fields[0], fields[1],
                        fields[2], fields[3], fields[4]));
            }
        }

        // Return path to output file
        return tacPath;
    }

}
