package eu.fbk.microneel.util;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import com.google.common.base.Throwables;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A model for classifying Twitter usernames into Null, Organization and Person
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PersonOrgClassifier {
    private static final Logger logger = LoggerFactory.getLogger(PersonOrgClassifier.class);
    private final HashMap<String, Integer> firstNames = new HashMap<>();
    private final HashMap<String, Integer> lastNames = new HashMap<>();
    private final HashMap<String, Integer> firstNamesExpanded = new HashMap<>();
    private final HashMap<String, Integer> lastNamesExpanded = new HashMap<>();
    private final HashMap<String, HashMap<String, Integer>> sets = new HashMap<String, HashMap<String, Integer>>() {{
        this.put("first", firstNames);
        this.put("last", lastNames);
        this.put("first expanded", firstNamesExpanded);
        this.put("last expanded", lastNamesExpanded);
    }};

    public void addNames(CSVRecord record) {
        addFirstName(record.get(1));
        addLastName(record.get(2));
    }

    public void cutWithThreshold(int threshold) {
        for (Map.Entry<String, HashMap<String, Integer>> set : sets.entrySet()) {
            logger.info("Filtering nameset: "+set.getKey());
            cutWithThreshold(threshold, set.getValue());
        }
    }

    public void cutWithThreshold(int threshold, HashMap<String, Integer> map) {
        logger.info("  Before filtering: "+map.size());
        Iterator<Map.Entry<String, Integer>> entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Integer> entry = entries.next();
            if (entry.getValue() < threshold) {
                entries.remove();
            }
        }
        logger.info("  After filtering: "+map.size());
    }

    public void addFirstName(String name) {
        addName(name, firstNames, firstNamesExpanded);
    }

    public void addLastName(String name) {
        addName(name, lastNames, lastNamesExpanded);
    }

    private void addName(String name, HashMap<String, Integer> map1, HashMap<String, Integer> map2) {
        name = name.toLowerCase().trim();
        if (name.length() < 3) {
            return;
        }
        addNameToMap(name, map1);
        for (String part : parseName(name)) {
            addNameToMap(part, map2);
        }
    }

    private void addNameToMap(String name, HashMap<String, Integer> map) {
        map.put(name, map.getOrDefault(name, 0)+1);
    }

    private List<String> parseName(String name) {
        List<String> result = new LinkedList<>();
        result.add(name);

        String[] subnames = name.split(" ");
        for (String subname : subnames) {
            subname = trim(subname);
            if (subname.length() < 3) {
                continue;
            }
            result.add(subname);
        }

        return result;
    }

    private String trim(String string) {
        int beg = 0, end = string.length();

        while (beg < end && checkSymbol(string.charAt(beg))) {
            beg++;
        }
        while (beg < end && checkSymbol(string.charAt(end-1))) {
            end--;
        }
        return beg > 0 && end < string.length() ? string.substring(beg, end) : string;
    }

    private static final char[] SYMBOLS = new char[]{'"', '\'', ' ', '_'};
    private boolean checkSymbol(char ch) {
        for (char symbol : SYMBOLS) {
            if (ch == symbol) {
                return true;
            }
        }
        return false;
    }

    public void extractFeatures(File dataset, File output) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(dataset), CSVFormat.DEFAULT.withDelimiter('\t'));
        FileWriter svmOutput = new FileWriter(output);

        boolean first = true;
        for (CSVRecord record : parser) {
            int label = getLabel(record.get(1));
            int[] features = getFeatures(record.get(0));

            if (first) {
                first = false;
            } else {
                svmOutput.write('\n');
            }
            svmOutput.write(String.valueOf(label));
            for (int i = 0; i < features.length; i++) {
                svmOutput.write(" "+(i+1)+":"+features[i]);
            }
        }
        parser.close();
        svmOutput.close();
    }

    private static final int UNIQUE_FEATURES = 8;
    private int[] getFeatures(String username) {
        int[] features = new int[UNIQUE_FEATURES + (UNIQUE_FEATURES * (UNIQUE_FEATURES - 1)) / 2];
        Arrays.fill(features, 0);
        Collection<String> parts = breakUsername(username);
        double random = new Random().nextDouble();
        if (random < 0.4) {
            logger.info("Username: "+username+". Parts: "+String.join(", ", parts));
        }

        for (String part : parts) {
            if (firstNames.containsKey(part)) {
                features[0] = 1;
            }
            if (lastNames.containsKey(part)) {
                features[1] = 1;
            }
            if (firstNamesExpanded.containsKey(part)) {
                features[2] = 1;
            }
            if (lastNamesExpanded.containsKey(part)) {
                features[3] = 1;
            }
        }

        int featureId = 4;
        for (Map.Entry<String, HashMap<String, Integer>> set : sets.entrySet()) {
            for (String name : set.getValue().keySet()) {
                if (username.toLowerCase().contains(name)) {
                    if (random < 0.4) {
                        logger.info("  Matched["+set.getKey()+", freq: "+set.getValue().get(name)+"]: " + name);
                    }
                    features[featureId] = 1;
                }
            }
            featureId++;
        }

        int idx = UNIQUE_FEATURES;
        for (int i = 0; i < UNIQUE_FEATURES; i++) {
            for (int j = i+1; j < UNIQUE_FEATURES; j++) {
                features[idx] = features[i] * features[j];
                idx++;
            }
        }
        return features;
    }

    private Collection<String> breakUsername(String username) {
        Set<String> result = new HashSet<>();
        for (String part : username.split("[0-9]+|_")) {
            result.addAll(breakOnCamelCase(part));
        }
        return result;
    }

    private LinkedList<String> breakOnCamelCase(String username) {
        LinkedList<String> parts = new LinkedList<>();
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

    private int getLabel(String label) {
        switch (label) {
            case "Person":
                return 1;
            case "Organization":
                return 2;
            default:
            case "Null":
                return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        TrainConfiguration config = loadTrainConfig(args);
        if (config == null) {
            logger.error("Unable to read configuration");
            return;
        }

        PersonOrgClassifier script = new PersonOrgClassifier();
        for (String file : config.input) {
            CSVParser parser = new CSVParser(new FileReader(new File(file)), CSVFormat.MYSQL);
            for (CSVRecord record : parser) {
                script.addNames(record);
            }
            parser.close();
        }
        script.cutWithThreshold(config.freqThreshold);
        script.extractFeatures(new File(config.dataset), new File(config.output));
    }

    private static class TrainConfiguration {
        String[] input;
        String dataset;
        String output;
        int freqThreshold = 150;
    }

    private static TrainConfiguration loadTrainConfig(String[] args) {
        Options options = new Options();
        options.addOption(
            Option.builder("o").desc("Output file with features")
                .required().hasArg().argName("file").longOpt("output").build()
        );
        options.addOption(
            Option.builder("d").desc("Input training dataset")
                .required().hasArg().argName("file").longOpt("dataset").build()
        );
        options.addOption(
            Option.builder("t").desc("Frequency threshold for names")
                .hasArg().argName("frequency").longOpt("threshold").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            TrainConfiguration configuration = new TrainConfiguration();
            configuration.output = line.getOptionValue("output");
            configuration.dataset = line.getOptionValue("dataset");
            if (line.hasOption("threshold")) {
                configuration.freqThreshold = Integer.valueOf(line.getOptionValue("threshold"));
            }
            configuration.input = line.getArgs();
            return configuration;
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed: " + exp.getMessage() + "\n");
            printHelp(options);
            System.exit(1);
        }
        return null;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
            200,
            "java -Dfile.encoding=UTF-8 "+PersonOrgClassifier.class.getName(),
            "\n",
            options,
            "\n",
            true
        );
    }

    private static Object call(final Object object, final String methodName, final Object... args) {
        final boolean isStatic = object instanceof Class<?>;
        final Class<?> clazz = isStatic ? (Class<?>) object : object.getClass();
        for (final Method method : clazz.getMethods()) {
            if (method.getName().equals(methodName)
                    && isStatic == Modifier.isStatic(method.getModifiers())
                    && method.getParameterTypes().length == args.length) {
                try {
                    return method.invoke(isStatic ? null : object, args);
                } catch (final InvocationTargetException ex) {
                    Throwables.propagate(ex.getCause());
                } catch (final IllegalAccessException ex) {
                    throw new IllegalArgumentException("Cannot invoke " + method, ex);
                }
            }
        }
        throw new IllegalArgumentException("Cannot invoke " + methodName);
    }
}
