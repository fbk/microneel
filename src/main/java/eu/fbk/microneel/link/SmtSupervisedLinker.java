package eu.fbk.microneel.link;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import eu.fbk.microneel.Annotator;
import eu.fbk.microneel.Category;
import eu.fbk.microneel.Post;
import eu.fbk.utils.svm.Classifier;
import eu.fbk.utils.svm.LabelledVector;
import eu.fbk.utils.svm.Vector;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SmtSupervisedLinker implements Annotator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmtSupervisedLinker.class);

    Map<String, Map<Integer, String>> gold = new HashMap<>();

    private static String modelFilePattern = "smtlinker.classification.model";
    private Path modelFile;
    private boolean doTrain = false;
    private Integer crossValidation = null;
    private Classifier.Parameters parameters;
    private static int DEFAULT_THRESHOLD = 100;

    private static final char[] SYMBOLS = new char[] { '"', '\'', ' ', '_' };
    private final HashMap<String, Integer> firstNames;
    private final HashMap<String, Integer> lastNames;
    private final HashMap<String, Integer> firstNamesExpanded;
    private final HashMap<String, Integer> lastNamesExpanded;
    private final Map<String, Map<String, Integer>> sets;
    private final Map<Map<String, Integer>, Integer> maxFrequencies;

    public SmtSupervisedLinker(final JsonObject json, Path configDir) {
        BufferedReader reader;
        String line;

        parameters = Classifier.Parameters.forSVMPolyKernel(3, null, null, null, null, null);

        if (json.has("train")) {
            doTrain = json.get("train").getAsBoolean();
        }
        if (json.has("cross")) {
            crossValidation = json.get("cross").getAsInt();
        }
        modelFile = configDir.resolve(modelFilePattern);
        int freqThreshold = DEFAULT_THRESHOLD;
        if (json.has("freqThreshold")) {
            freqThreshold = json.get("freqThreshold").getAsInt();
        }

        Path goldFile = configDir.resolve("training.annotations.tsv");
        Path nameDataIndex = configDir.resolve("data/personinfo/it.csv");

        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.firstNames = new HashMap<>();
        this.lastNames = new HashMap<>();
        this.firstNamesExpanded = new HashMap<>();
        this.lastNamesExpanded = new HashMap<>();
        this.maxFrequencies = new HashMap<>();
        this.sets = new HashMap<>();
        this.sets.put("first", this.firstNames);
        this.sets.put("last", this.lastNames);
        this.sets.put("first_expanded", this.firstNamesExpanded);
        this.sets.put("last_expanded", this.lastNamesExpanded);

//        for (final Path path : paths) {
//            final CSVParser parser = new CSVParser(new FileReader(path.toFile()), CSVFormat.MYSQL);
        try {
            final CSVParser parser = new CSVParser(new FileReader(nameDataIndex.toFile()), CSVFormat.MYSQL);
            for (final CSVRecord record : parser) {
                addName(record.get(1), this.firstNames, this.firstNamesExpanded);
                addName(record.get(2), this.lastNames, this.lastNamesExpanded);
            }
            parser.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        }
        for (final Map.Entry<String, Map<String, Integer>> set : this.sets.entrySet()) {
            LOGGER.debug("Filtering nameset: " + set.getKey());
            final Map<String, Integer> map = set.getValue();
            LOGGER.debug("  Before filtering: " + map.size());
            final Iterator<Map.Entry<String, Integer>> entries = map.entrySet().iterator();
            int max = 0;
            while (entries.hasNext()) {
                final Map.Entry<String, Integer> entry = entries.next();
                if (max < entry.getValue()) {
                    max = entry.getValue();
                }
                if (entry.getValue() < freqThreshold) {
                    entries.remove();
                }
            }
            LOGGER.debug("  After filtering: " + map.size());
            this.maxFrequencies.put(map, max);
        }

    }

    @Override public void annotate(Iterable<Post> posts) throws Throwable {
        Post[] postArray = Iterables.toArray(posts, Post.class);

        if (crossValidation != null) {
            LOGGER.info("Cross-validation, size {}", crossValidation);

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
                LOGGER.info("Saving model: " + modelFile.toAbsolutePath());
                classifier.writeTo(modelFile);
            } else {
                LOGGER.info("Reading model: " + modelFile.toAbsolutePath());
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
                final Vector.Builder builder = Vector.builder();
                builder.set(features.get(offset));

                int label = 0;
                if (gold.get(id) != null) {
                    if (gold.get(id).get(offset + 1) != null) {
                        switch (gold.get(id).get(offset + 1)) {
                        case "Person":
                            label = 1;
                            break;
                        case "Organization":
                        case "Product":
                            label = 2;
                            break;
                        }
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
            final Vector.Builder builder = Vector.builder();
            builder.set(features.get(offset));

            // Predict type (SVM)
            Vector vector = builder.build();
            LabelledVector predict = classifier.predict(false, vector);

            Integer label = predict.getLabel();
            Category category = null;
            switch (label) {
            case 1:
                category = Category.PERSON;
                break;
            case 2:
                category = Category.ORGANIZATION;
                break;
            }
            if (category != null) {
                Post.MentionAnnotation ma = post.getAnnotations(offset, Post.MentionAnnotation.class);
                try {
                    final Post.EntityAnnotation a = post.addAnnotation(Post.EntityAnnotation.class,
                            ma.getBeginIndex() + 1, ma.getEndIndex(), "smt");
                    a.setCategory(category);
                    a.setUri(ma.getUri());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private void merge(List<Post> testPosts, List<Post> trainingPosts, int i, int split) {

        Classifier classifier = null;
        LOGGER.debug("Training set size: " + trainingPosts.size());
        LOGGER.debug("Test set size: " + testPosts.size());

        try {
            classifier = trainClassifier(trainingPosts, String.format("Perform linker training (%d/%d)", i + 1, split));
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Post testPost : testPosts) {
            annotatePost(classifier, testPost);
        }
    }

    private HashMultimap<Integer, String> extractFeatures(Post post) {
        HashMultimap<Integer, String> features = HashMultimap.create();
        for (final Post.MentionAnnotation ma : post.getAnnotations(Post.MentionAnnotation.class)) {
            features.putAll(ma.getBeginIndex(), extractFeatures(ma));
        }
        return features;
    }

    private Set<String> extractFeatures(final Post.MentionAnnotation annotation) {

        // Fields features are extracted from
        final String username = annotation.getUsername();
        final String lang = annotation.getLang();
        final String uri = annotation.getUri();
        final Category uriCategory = annotation.getCategory();

        // Feature initialization
        final Set<String> features = Sets.newHashSet();
        final Collection<String> parts = breakUsername(username);

        // Log reporting
        final double random = new Random().nextDouble();
        if (random < 0.4) {
            LOGGER.debug("Username: " + username + ", name: "
                    + (annotation.getFullName() == null ? "" : annotation.getFullName()));
            LOGGER.debug("  Username parts: " + String.join(", ", parts));
            LOGGER.debug("  Name parts: " + String.join(", ", parts));
        }

        // Filling array of features
        for (final String part : parts) {
            for (final Map.Entry<String, Map<String, Integer>> entry : this.sets.entrySet()) {
                final String setId = entry.getKey();
                final Map<String, Integer> set = entry.getValue();
                if (set.containsKey(part)) {
                    features.add("part_in_" + setId);
                }
            }
        }

        features.add(uri == null ? "uri_null" : "uri_exists");
        features.add(lang == null ? "lang_null" : "lang_" + lang.toLowerCase());
        features.add(uriCategory == null ? "uri_category_null"
                : "uri_category_" + uriCategory.toString().toLowerCase());

        for (final Map.Entry<String, Map<String, Integer>> entry : this.sets.entrySet()) {
            final String setId = entry.getKey();
            for (final String name : entry.getValue().keySet()) {
                if (username.toLowerCase().contains(name)) {
                    if (random < 0.4) {
                        LOGGER.debug("  Matched[" + entry.getKey() + ", freq: "
                                + entry.getValue().get(name) + "]: " + name);
                    }
                    features.add("contains_" + setId);
                }
            }
        }

        // Combinations of features
        final List<String> uniqueFeatures = new ArrayList<>(features);
        for (final String feature1 : uniqueFeatures) {
            for (final String feature2 : uniqueFeatures) {
                if (feature1.compareTo(feature2) < 0) {
                    features.add(feature1 + "_AND_" + feature2);
                }
            }
        }
        return features;
    }

    private static void addName(String name, final HashMap<String, Integer> map1,
            final HashMap<String, Integer> map2) {
        name = name.toLowerCase().trim();
        if (name.length() < 3) {
            return;
        }
        map1.put(name, map1.getOrDefault(name, 0) + 1);
        for (final String part : parseName(name)) {
            map2.put(part, map2.getOrDefault(name, 0) + 1);
        }
    }

    private static List<String> parseName(final String name) {
        final List<String> result = new LinkedList<>();
        result.add(name);
        final String[] subnames = name.split(" ");
        for (String subname : subnames) {
            subname = trim(subname);
            if (subname.length() < 3) {
                continue;
            }
            result.add(subname);
        }
        return result;
    }

    private static String trim(final String string) {
        int beg = 0, end = string.length();
        while (beg < end && checkSymbol(string.charAt(beg))) {
            beg++;
        }
        while (beg < end && checkSymbol(string.charAt(end - 1))) {
            end--;
        }
        return beg > 0 && end < string.length() ? string.substring(beg, end) : string;
    }

    private static boolean checkSymbol(final char ch) {
        for (final char symbol : SYMBOLS) {
            if (ch == symbol) {
                return true;
            }
        }
        return false;
    }

    private static Collection<String> breakUsername(final String username) {
        final Set<String> result = new HashSet<>();
        for (final String part : username.split("[0-9]+|_")) {
            result.addAll(breakOnCamelCase(part));
        }
        return result;
    }

    private static LinkedList<String> breakOnCamelCase(final String username) {
        final LinkedList<String> parts = new LinkedList<>();
        if (username.length() == 0) {
            return parts;
        }
        parts.add(username.toLowerCase());

        int lastI = 0;
        for (int i = 1; i < username.length(); i++) {
            if (Character.isUpperCase(username.charAt(i))) {
                parts.add(username.substring(lastI, i).toLowerCase());
                lastI = i;
            }
        }
        if (lastI != 0) {
            parts.add(username.substring(lastI).toLowerCase());
        }
        return parts;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
