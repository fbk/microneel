package eu.fbk.microneel.link;

import com.google.gson.JsonObject;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import eu.fbk.dh.tint.runner.TintPipeline;
import eu.fbk.dkm.pikes.tintop.annotators.raw.LinkingTag;
import eu.fbk.dkm.pikes.tintop.annotators.raw.MachineLinking;
import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.microneel.util.Rewriting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by alessio on 14/09/16.
 */

public class TintLinker implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TintLinker.class);
    private static String destinationLang = "en";
    private static String destinationPattern = "http://dbpedia.org/resource/%s";

    MachineLinking machineLinking;
    Properties properties;
    HashMap<String, String> translations = new HashMap<>();

    public TintLinker(final JsonObject json, Path configDir) {
        try {
            Path wikiDataIndex = configDir.resolve("data/wikidata/it.csv");

            LOGGER.info("Reading translations file");
            BufferedReader reader = new BufferedReader(new FileReader(wikiDataIndex.toFile()));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                String[] parts = line.split("\t");
                String orig = parts[0];
                for (String part : parts) {
                    if (part.startsWith(destinationLang + ":")) {
                        String translation = part.substring(destinationLang.length() + 1);
                        translations.put(orig, translation);
                    }
                }
            }
            reader.close();

            Path propFile = configDir.resolve("tint.properties");
            InputStream configStream = new FileInputStream(propFile.toFile());
            properties = new Properties();
            properties.load(configStream);

            TintPipeline pipeline = new TintPipeline();
            pipeline.loadDefaultProperties();
            pipeline.addProperties(properties);
            pipeline.load();

            Properties mlProperties = new Properties();
            mlProperties.setProperty("address", "http://ml.apnetwork.it/annotate");
            mlProperties.setProperty("min_confidence", "0.25");
            machineLinking = new MachineLinking(mlProperties);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override public void annotate(Post post) throws Throwable {
        Rewriting rewriting = post.getRewriting();
        String id = post.getId();
        String text = rewriting.getRewrittenString();
        String mlText = text;

        TintPipeline pipeline = new TintPipeline();
        pipeline.loadDefaultProperties();
        pipeline.addProperties(properties);
        pipeline.load();

        String tweetLang = machineLinking.lang(text);

        // Adding mention context
        List<Post.MentionAnnotation> mentionAnnotations = post.getAnnotations(Post.MentionAnnotation.class);
        for (Post.MentionAnnotation mentionAnnotation : mentionAnnotations) {

            String description = mentionAnnotation.getDescription();
            if (description == null) {
                continue;
            }
            description = description.trim();
            if (description.length() == 0) {
                continue;
            }

            try {
                String lang = machineLinking.lang(description);
                if (lang.equals(tweetLang)) {
                    mlText += " " + description;
                }
            } catch (Exception e) {
                // continue
            }
        }

        // Adding hashtags definitions
        List<Post.HashtagAnnotation> hashtagAnnotations = post.getAnnotations(Post.HashtagAnnotation.class);
        for (Post.HashtagAnnotation hashtagAnnotation : hashtagAnnotations) {
            Set<String> definitions = hashtagAnnotation.getDefinitions();
            if (definitions == null) {
                continue;
            }

            for (String definition : definitions) {
                try {
                    String lang = machineLinking.lang(definition);
                    if (lang.equals(tweetLang)) {
                        mlText += " " + definition;
                    }
                } catch (Exception e) {
                    // continue
                }
            }

        }

        // Adding URL titles
        List<Post.UrlAnnotation> urlAnnotations = post.getAnnotations(Post.UrlAnnotation.class);
        for (Post.UrlAnnotation urlAnnotation : urlAnnotations) {
            String title = urlAnnotation.getTitle();
            if (title == null) {
                continue;
            }
            title = title.trim();
            if (title.length() == 0) {
                continue;
            }

            try {
                String lang = machineLinking.lang(title);
                if (lang.equals(tweetLang)) {
                    mlText += " " + title;
                }
            } catch (Exception e) {
                // continue
            }
        }

        // Running Tint
        int lastIndex = -1;
        Map<Integer, Integer> indexes = new HashMap<>();
        Map<Integer, String> ners = new HashMap<>();

        Annotation annotation = pipeline.runRaw(text);

        List<CoreLabel> tokens = annotation.get(CoreAnnotations.TokensAnnotation.class);
        for (CoreLabel token : tokens) {
            String ner = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

            int begin = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
            int end = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);

            String lastNer = ners.get(lastIndex);
            if (lastNer == null || !lastNer.equals(ner)) {
                indexes.put(begin, end);
                ners.put(begin, ner);
                lastIndex = begin;
            } else {
                indexes.put(lastIndex, end);
            }
        }

        // Linking
        List<LinkingTag> linkingTags = new ArrayList<>();
        try {
            linkingTags = machineLinking.tag(mlText);
        } catch (Exception e) {
            // continue
        }

        // Checking NER
        for (Integer begin : ners.keySet()) {
            String ner = ners.get(begin);
            if (ner.equals("O")) {
                continue;
            }
            Integer end = indexes.get(begin);

            int originalBegin = -1;
            int originalEnd = -1;
            try {
                originalBegin = rewriting.toOriginalOffset(begin);
                originalEnd = rewriting.toOriginalOffset(end);
                Post.EntityAnnotation entityAnnotation = post
                        .addAnnotation(Post.EntityAnnotation.class, originalBegin, originalEnd, "stanford");
                switch (ner) {
                case "PER":
                    entityAnnotation.setCategory(Category.PERSON);
                    break;
                case "LOC":
                    entityAnnotation.setCategory(Category.LOCATION);
                    break;
                case "ORG":
                    entityAnnotation.setCategory(Category.ORGANIZATION);
                    break;
                }
            } catch (Exception e) {
                LOGGER.warn("Entity conflict in NER ({}) - ({}, {}) -> ({}, {}) - {}", e.getMessage(), begin, end, originalBegin, originalEnd, post.getRewriting());
            }
        }

        // Checking linking
        for (LinkingTag linkingTag : linkingTags) {
            Integer begin = linkingTag.getOffset();
            Integer end = linkingTag.getOffset() + linkingTag.getLength();
            String dbpediaEntity = linkingTag.getPage();
            double score = linkingTag.getScore();

            // Remove context
            if (end > text.length()) {
                continue;
            }

            // todo: check score?

            HashMap<LinkingTag.Category, HashSet<String>> types = linkingTag.getTypes();
            HashSet<String> airpediaTypes = types.get(LinkingTag.Category.DBPEDIA);

            if (airpediaTypes == null) {
                continue;
            }

            Category type = null;
            if (airpediaTypes.contains("Work")) {
                type = Category.PRODUCT;
            }
            if (airpediaTypes.contains("Person")) {
                type = Category.PERSON;
            }
            if (airpediaTypes.contains("Organisation")) {
                type = Category.ORGANIZATION;
            }
            if (airpediaTypes.contains("Place")) {
                type = Category.LOCATION;
            }
            if (airpediaTypes.contains("Event")) {
                type = Category.EVENT;
            }

            if (type == null) {
                continue;
            }

            // Search for translation
            String wikipediaEntity = dbpediaEntity.replaceAll("http://.*\\.dbpedia\\.org/[a-z]+/", "");
            String translation = translations.get(wikipediaEntity);
            if (translation != null) {
                dbpediaEntity = String.format(destinationPattern, translation);
            }

            int originalBegin = rewriting.toOriginalOffset(begin);
            int originalEnd = rewriting.toOriginalOffset(end);

            try {
                Post.EntityAnnotation entityAnnotation = post
                        .addAnnotation(Post.EntityAnnotation.class, originalBegin, originalEnd, "ml");
                entityAnnotation.setCategory(type);
                entityAnnotation.setUri(dbpediaEntity);
            } catch (Exception e) {
                LOGGER.warn("Entity conflict in linking ({}) - ({}, {}) -> ({}, {}) - {}", e.getMessage(), begin, end, originalBegin, originalEnd, post.getRewriting());
            }
        }
    }
}
