package eu.fbk.microneel.link;

import com.google.gson.JsonObject;
import edu.stanford.nlp.pipeline.Annotation;
import eu.fbk.dh.tint.runner.TintPipeline;
import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.util.Rewriting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by alessio on 14/09/16.
 */

public class TintLinker implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TintLinker.class);
    TintPipeline pipeline;

    public TintLinker(final JsonObject json, Path configDir) {
        try {
            Path propFile = configDir.resolve("tint.properties");
            InputStream configStream = new FileInputStream(propFile.toFile());

            pipeline = new TintPipeline();
            pipeline.loadPropertiesFromStream(configStream);
            pipeline.loadDefaultProperties();
            pipeline.load();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override public void annotate(Post post) throws Throwable {
        Rewriting rewriting = post.getRewriting();
        String text = rewriting.getRewrittenString();
        String mlText = text;

        Annotation annotation = pipeline.runRaw(text);

        // passo testo in Tint

        // aggiungere contesto delle menzioni
        List<Post.MentionAnnotation> mentionAnnotations = post.getAnnotations(Post.MentionAnnotation.class);
        for (Post.MentionAnnotation mentionAnnotation : mentionAnnotations) {
            System.out.println(mentionAnnotation.getLang());
            String description = mentionAnnotation.getDescription();
            if (description != null) {
                mlText += " " + description;
            }
        }

        // definizioni HASHTAGS (controllare lingua, perché molti sono in inglese)
        List<Post.HashtagAnnotation> hashtagAnnotations = post.getAnnotations(Post.HashtagAnnotation.class);

        // controllare la lingua
        List<Post.UrlAnnotation> urlAnnotations = post.getAnnotations(Post.UrlAnnotation.class);

//        int beginOffset = 5; // metterci inizio/fine
//        int endOffset = 10;
//
//        int originalBegin = rewriting.toOriginalOffset(5);
//        int originalEnd = rewriting.toOriginalOffset(10);
//
//        // Convertire URI di DBpedia in inglese, altrimenti restituire quella italiana
//
//        try {
//            Post.EntityAnnotation entityAnnotation = post
//                    .addAnnotation(Post.EntityAnnotation.class, originalBegin, originalEnd);
//            entityAnnotation.setUri("DBpedia URI");
//            entityAnnotation.setCategory(Category.PERSON);
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println(originalBegin);
//            System.out.println(originalEnd);
//            System.out.println(post.toString());
//            System.exit(1);
//        }
    }
}
