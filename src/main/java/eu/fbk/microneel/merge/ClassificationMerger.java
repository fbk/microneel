package eu.fbk.microneel.merge;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.utils.core.FrequencyHashSet;
import eu.fbk.utils.svm.Classifier;
import eu.fbk.utils.svm.LabelledVector;
import eu.fbk.utils.svm.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class ClassificationMerger implements Annotator {

    //    private static int split = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassificationMerger.class);

    private static Map<Category, Integer> outcome = new HashMap<>();
    private static Map<Integer, Category> classificationCategories = new HashMap<>();
//    private Map<String, FrequencyHashSet<String>> tokens = new HashMap<>();
//    private TintPipeline pipeline;

    static {
        outcome.put(Category.PERSON, 1);
        outcome.put(Category.ORGANIZATION, 2);
        outcome.put(Category.LOCATION, 3);
        outcome.put(Category.PRODUCT, 4);
        outcome.put(Category.EVENT, 5);
        outcome.put(Category.CHARACTER, 6);
        outcome.put(Category.THING, 7);
    }

    private final String[] qualifiers;
    Map<String, Map<Integer, String>> gold = new HashMap<>();

    private static String modelFilePattern = "classification.model";
    private Path modelFile;
    private boolean doTrain = false;
    private Integer crossValidation = null;
    private Classifier.Parameters parameters;

    public ClassificationMerger(final JsonObject json, Path configDir) {
        BufferedReader reader;
        String line;

        parameters = Classifier.Parameters
                .forSVMPolyKernel(outcome.size() + 1, null, null, null, null, null);

        if (json.has("train")) {
            doTrain = json.get("train").getAsBoolean();
        }
        if (json.has("cross")) {
            crossValidation = json.get("cross").getAsInt();
        }
        modelFile = configDir.resolve(modelFilePattern);

        if (!json.has("q")) {
            this.qualifiers = new String[] {};
        } else {
            final JsonArray array = json.get("q").getAsJsonArray();
            this.qualifiers = new String[array.size()];
            for (int i = 0; i < array.size(); ++i) {
                this.qualifiers[i] = array.get(i).getAsString();
            }
        }

        for (Category category : outcome.keySet()) {
            classificationCategories.put(outcome.get(category), category);
        }

        Path goldFile = configDir.resolve("training.annotations.tsv");

//        Map<String, Path> files = new HashMap<>();
//        files.put("loc", configDir.resolve("data/ner/it-LOC-tokens.txt"));
//        files.put("per", configDir.resolve("data/ner/it-PER-tokens.txt"));
//        files.put("org", configDir.resolve("data/ner/it-ORG-tokens.txt"));

        try {
//            Path propFile = configDir.resolve("tint.properties");
//            InputStream configStream = new FileInputStream(propFile.toFile());
//            Properties properties = new Properties();
//            properties.load(configStream);
//            properties.setProperty("annotators", "ita_toksent");
//
//            pipeline = new TintPipeline();
//            pipeline.loadDefaultProperties();
//            pipeline.addProperties(properties);
//            pipeline.load();

            LOGGER.info("Loading gold file");
            reader = new BufferedReader(new FileReader(goldFile.toFile()));
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                String[] parts = line.split("\t");
                String neelID = "twitter:" + parts[0];
                gold.putIfAbsent(neelID, new HashMap<>());
                String type = parts[4];
                gold.get(neelID).put(Integer.parseInt(parts[1]), type);
            }
            reader.close();

//            for (String key : files.keySet()) {
//                LOGGER.info("Loading {} file", key.toUpperCase());
//                Path thisFile = files.get(key);
//                tokens.put(key, new FrequencyHashSet<>());
//                reader = new BufferedReader(new FileReader(thisFile.toFile()));
//                while ((line = reader.readLine()) != null) {
//                    line = line.trim();
//                    if (line.length() == 0) {
//                        continue;
//                    }
//                    String[] parts = line.split("\\s+");
//                    for (int i = 1; i < parts.length; i++) {
//                        String part = parts[i];
//                        if (!Character.isUpperCase(part.charAt(0))) {
//                            continue;
//                        }
//                        if (part.length() <= 2) {
//                            continue;
//                        }
//                        tokens.get(key).add(part);
//                    }
//                }
//                reader.close();
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public ClassificationMerger(final String... qualifiers) {
        this.qualifiers = qualifiers.clone();
    }

    @Override public void annotate(Iterable<Post> posts) throws Throwable {
        Post[] postArray = Iterables.toArray(posts, Post.class);

        if (crossValidation != null) {
            int split = crossValidation;
            int size = postArray.length;
            int step = size / split;

            for (int i = 0; i < split; i++) {
                int start = step * i;
                int end = start + step;
                if (i == split - 1) {
                    end = size;
                }

                List<Post> trainingPosts = new ArrayList<>();
                List<Post> testPosts = new ArrayList<>();

                for (int j = 0; j < size; j++) {
                    Post thisPost = postArray[j];
                    if (j >= start && j < end) {
                        testPosts.add(thisPost);
                    } else {
                        trainingPosts.add(thisPost);
                    }
                }

                merge(testPosts, trainingPosts, i, split);
            }
        } else {
            Classifier classifier;
            if (doTrain) {
                classifier = trainClassifier(Arrays.asList(postArray));
                classifier.writeTo(modelFile);
            } else {
                classifier = Classifier.readFrom(modelFile);
                for (Post post : postArray) {
                    annotatePost(classifier, post);
                }
            }
        }
    }

    private Classifier trainClassifier(List<Post> posts) throws IOException {
        return trainClassifier(posts, null);
    }

    private Classifier trainClassifier(List<Post> posts, String logMessage) throws IOException {
        List<LabelledVector> trainingSet = new ArrayList<>();
        for (Post trainingPost : posts) {
            HashMultimap<Integer, String> features = extractFeatures(trainingPost);
            String id = trainingPost.getId();

            for (Integer offset : features.keySet()) {
                final Vector.Builder builder = eu.fbk.utils.svm.Vector.builder();
                builder.set(features.get(offset));

                int label = 0;
                if (gold.get(id) != null) {
                    if (gold.get(id).get(offset) != null) {
                        label = outcome.get(Category.valueOf(gold.get(id).get(offset).toUpperCase()));
                    }
                }

                LabelledVector vector = builder.build().label(label);
                trainingSet.add(vector);
            }
        }

        if (logMessage != null) {
            LOGGER.info(logMessage);
        }
        return Classifier.train(parameters, trainingSet);

    }

    private void annotatePost(Classifier classifier, Post post) {
        HashMultimap<Integer, String> features = extractFeatures(post);
        for (Integer offset : features.keySet()) {
            final eu.fbk.utils.svm.Vector.Builder builder = eu.fbk.utils.svm.Vector.builder();
            builder.set(features.get(offset));

            // Predict end (rule-based)
            int end = -1;
            FrequencyHashSet<String> urls = new FrequencyHashSet<>();
            for (String qualifier : qualifiers) {
                Post.EntityAnnotation annotation = post
                        .getAnnotation(offset, Post.EntityAnnotation.class, qualifier);
                if (annotation != null) {
                    if (end == -1) {
                        end = annotation.getEndIndex();
                    } else {
                        end = Math.min(end, annotation.getEndIndex());
                    }
                    String uri = annotation.getUri();
                    if (uri != null) {
                        urls.add(uri);
                    }
                }
            }

            // Predict type (SVM)
            eu.fbk.utils.svm.Vector vector = builder.build();
            LabelledVector predict = classifier.predict(false, vector);

            // Predict label (rule-based)
            try {
                if (predict.getLabel() != 0) {
                    final Post.EntityAnnotation ta = post
                            .addAnnotation(Post.EntityAnnotation.class, offset, end);
                    ta.setCategory(classificationCategories.get(predict.getLabel()));
                    if (urls.size() > 0) {
                        String candidate = urls.mostFrequent();
                        if (urls.get(candidate) > 0) {
                            ta.setUri(candidate);
                        }
                    }
                }
            } catch (final Throwable ex) {
                // Ignore
            }
        }

    }

    private void merge(List<Post> testPosts, List<Post> trainingPosts, int i, int split) {

        Classifier classifier = null;

        try {
            classifier = trainClassifier(trainingPosts, String.format("Perform training (%d/%d)", i + 1, split));
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Post testPost : testPosts) {
            annotatePost(classifier, testPost);
        }
    }

    private HashMultimap<Integer, String> extractFeatures(Post post) {
        HashMultimap<Integer, String> features = HashMultimap.create();

        // Mentions
        Set<Integer> mentionBeginIndexes = new HashSet<>();
        Set<Integer> mentionAllIndexes = new HashSet<>();
        List<Post.MentionAnnotation> mentionAnnotations = post.getAnnotations(Post.MentionAnnotation.class);
        for (Post.MentionAnnotation mentionAnnotation : mentionAnnotations) {
            mentionBeginIndexes.add(mentionAnnotation.getBeginIndex() + 1);
            for (int i = mentionAnnotation.getBeginIndex() + 1; i < mentionAnnotation.getEndIndex(); i++) {
                mentionAllIndexes.add(i);
            }
        }

        // Hashtags
        Set<Integer> hashtagBeginIndexes = new HashSet<>();
        Set<Integer> hashtagAllIndexes = new HashSet<>();
        List<Post.HashtagAnnotation> hashtagAnnotations = post.getAnnotations(Post.HashtagAnnotation.class);
        for (Post.HashtagAnnotation hashtagAnnotation : hashtagAnnotations) {
            hashtagBeginIndexes.add(hashtagAnnotation.getBeginIndex() + 1);
            for (int i = hashtagAnnotation.getBeginIndex() + 1; i < hashtagAnnotation.getEndIndex(); i++) {
                hashtagAllIndexes.add(i);
            }
        }

        Map<Integer, FrequencyHashSet<Category>> freqs = new HashMap<>();

        for (String qualifier : qualifiers) {
            List<Post.EntityAnnotation> annotations = post
                    .getAnnotations(Post.EntityAnnotation.class, qualifier);
            for (Post.EntityAnnotation annotation : annotations) {
                Category category = annotation.getCategory();
                int beginIndex = annotation.getBeginIndex();

                if (annotation.getBeginIndex() == annotation.getEndIndex()) {
                    System.out.println(annotation);
                    continue;
                }
                
//                Integer beginIndexRewritten = annotation.getBeginIndexRewritten();
//                Integer endIndexRewritten = annotation.getEndIndexRewritten();
//
//                if (beginIndexRewritten != null && endIndexRewritten != null) {
//                    String text = null;
//                    try {
//                        text = post.getRewriting().getRewrittenString()
//                                .substring(beginIndexRewritten, endIndexRewritten);
//                        if (post.getId().equals("twitter:288387899223855105")) {
//                            System.out.println(text);
//                        }
//
//                        Annotation tintAnnotation = pipeline.runRaw(text);
//                        for (CoreLabel token : tintAnnotation.get(CoreAnnotations.TokensAnnotation.class)) {
//                            String tokenText = token.originalText();
//                            for (String key : tokens.keySet()) {
//                                if (tokens.get(key).get(tokenText) != null) {
//                                    features.put(beginIndex, "contains_NER_" + key.toUpperCase());
//                                }
//                            }
//                        }
//                    } catch (Exception e) {
//                        // ignore
//                    }
//                }

                String uri = annotation.getUri();
                if (uri != null && uri.length() > 0) {
                    features.put(beginIndex, "isLinked");
                }

                features.put(beginIndex, category + "_" + qualifier);
                freqs.putIfAbsent(beginIndex, new FrequencyHashSet<>());
                freqs.get(beginIndex).add(category);

                features.put(beginIndex, qualifier + "_annotated");

                if (Character.isUpperCase(annotation.getSurfaceForm().charAt(0))) {
                    features.put(beginIndex, "uppercase");
                }

                if (mentionBeginIndexes.contains(beginIndex)) {
                    features.put(beginIndex, "isMention");
                }
                if (mentionAllIndexes.contains(beginIndex)) {
                    features.put(beginIndex, "isInMention");
                }
                if (hashtagBeginIndexes.contains(beginIndex)) {
                    features.put(beginIndex, "isHashtag");
                }
                if (hashtagAllIndexes.contains(beginIndex)) {
                    features.put(beginIndex, "isInHashtag");
                }
            }
        }

        for (Integer beginIndex : freqs.keySet()) {
            FrequencyHashSet<Category> typeFreq = freqs.get(beginIndex);
            for (Category category : typeFreq.keySet()) {
                int freq = typeFreq.get(category);
                for (int i = 0; i < freq; i++) {
                    features.put(beginIndex, category + "_freq_" + (i + 1));
                }
            }
        }

        return features;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}