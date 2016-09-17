package eu.fbk.microneel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import eu.fbk.utils.eval.ConfusionMatrix;
import eu.fbk.utils.eval.PrecisionRecall;
import eu.fbk.utils.svm.Classifier;
import eu.fbk.utils.svm.LabelledVector;
import eu.fbk.utils.svm.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by alessio on 17/09/16.
 */

public class MergeClassification {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergeClassification.class);

    public static void main(String[] args) {

        Path completeFile = (new File("neelit2016/training.linked.json.gz")).toPath();
        Path goldFile = (new File("neelit2016/training.annotations.original.tsv")).toPath();
        String qualifierString = "ml, stanford, smt";

        List<LabelledVector> trainingSet = Lists.newArrayList();

        Map<Category, Integer> outcome = new HashMap<>();
        outcome.put(Category.PERSON, 1);
        outcome.put(Category.ORGANIZATION, 2);
        outcome.put(Category.LOCATION, 3);
        outcome.put(Category.PRODUCT, 4);
        outcome.put(Category.EVENT, 5);
        outcome.put(Category.CHARACTER, 6);
        outcome.put(Category.THING, 7);

        try {

            Map<String, Map<Integer, String>> gold = new HashMap<>();

            BufferedReader reader;
            String line;

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

            String[] qualifiers = qualifierString.split("(\\s+)?,(\\s+)?");
            List<Post> posts = Post.read(completeFile);
            for (Post post : posts) {
                HashMultimap<Integer, String> features = HashMultimap.create();
                String id = post.getId();

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

                for (Integer offset : features.keySet()) {
                    final Vector.Builder builder = Vector.builder();
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

            final List<Classifier.Parameters> grid = Lists.newArrayList();
            for (final float weight : new float[] { 0.25f, 0.5f, 1.0f, 2.0f, 4.0f }) {
                grid.addAll(Classifier.Parameters.forLinearLRLossL1Reg(2,
                        new float[] { 1f, weight }, 1f, 1f).grid(Math.max(1, 25), 10.0f));
            }
            final Classifier classifier = Classifier.train(grid, trainingSet,
                    ConfusionMatrix.labelComparator(PrecisionRecall.Measure.F1, 1, true), 100000);

            System.out.println(trainingSet.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
