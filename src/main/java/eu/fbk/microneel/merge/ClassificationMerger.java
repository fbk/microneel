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
import java.nio.file.Path;
import java.util.*;

public class ClassificationMerger implements Annotator {

    private static int split = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassificationMerger.class);

    static Map<Category, Integer> outcome = new HashMap<>();
    static Map<Integer, Category> classificationCategories = new HashMap<>();

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

    public ClassificationMerger(final JsonObject json, Path configDir) {
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

        BufferedReader reader;
        String line;
        try {
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
//            totalGold++;
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ClassificationMerger(final String... qualifiers) {
        this.qualifiers = qualifiers.clone();
    }

    @Override public void annotate(Iterable<Post> posts) throws Throwable {
        Post[] postArray = Iterables.toArray(posts, Post.class);
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
    }

    private void merge(List<Post> testPosts, List<Post> trainingPosts, int i, int split) {

        List<LabelledVector> trainingSet = new ArrayList<>();
        for (Post trainingPost : trainingPosts) {
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

        Classifier classifier = null;

        try {
            LOGGER.info("Perform training ({}/{})", i + 1, split);
            Classifier.Parameters parameters = Classifier.Parameters.forSVMLinearKernel(outcome.size() + 1, null, null);
            classifier = Classifier.train(parameters, trainingSet);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        for (Post testPost : testPosts) {
            HashMultimap<Integer, String> features = extractFeatures(testPost);
            for (Integer offset : features.keySet()) {
                final eu.fbk.utils.svm.Vector.Builder builder = eu.fbk.utils.svm.Vector.builder();
                builder.set(features.get(offset));

                int end = -1;
                FrequencyHashSet<String> urls = new FrequencyHashSet<>();
                for (String qualifier : qualifiers) {
                    Post.EntityAnnotation annotation = testPost
                            .getAnnotation(offset, Post.EntityAnnotation.class, qualifier);
                    if (annotation != null) {
                        end = Math.max(end, annotation.getEndIndex());
                        String uri = annotation.getUri();
                        if (uri != null) {
                            urls.add(uri);
                        }
                    }
                }

                eu.fbk.utils.svm.Vector vector = builder.build();
                LabelledVector predict = classifier.predict(false, vector);

                try {
                    if (predict.getLabel() != 0) {
                        final Post.EntityAnnotation ta = testPost
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

//                System.out.println(offset);
//                System.out.println(features.get(offset));
//                System.out.println(predict);
//                System.out.println();
            }

        }

    }

    private HashMultimap<Integer, String> extractFeatures(Post post) {
        HashMultimap<Integer, String> features = HashMultimap.create();

        Set<Integer> mentionBeginIndexes = new HashSet<>();
        List<Post.MentionAnnotation> mentionAnnotations = post.getAnnotations(Post.MentionAnnotation.class);
        for (Post.MentionAnnotation mentionAnnotation : mentionAnnotations) {
            mentionBeginIndexes.add(mentionAnnotation.getBeginIndex() + 1);
        }

        for (String qualifier : qualifiers) {
            List<Post.EntityAnnotation> annotations = post
                    .getAnnotations(Post.EntityAnnotation.class, qualifier);
            for (Post.EntityAnnotation annotation : annotations) {
                Category category = annotation.getCategory();
                int beginIndex = annotation.getBeginIndex();
                features.put(beginIndex, category + "_" + qualifier);
                if (mentionBeginIndexes.contains(beginIndex)) {
                    features.put(beginIndex, "isMention");
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
