package eu.fbk.microneel.util;

import java.io.*;
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import com.google.gson.Gson;
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
    private final HashMap<HashMap<String, Integer>, Integer> maxFrequencies = new HashMap<>();

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
        int max = 0;
        while (entries.hasNext()) {
            Map.Entry<String, Integer> entry = entries.next();
            if (max < entry.getValue()) {
                max = entry.getValue();
            }
            if (entry.getValue() < threshold) {
                entries.remove();
            }
        }
        logger.info("  After filtering: "+map.size());
        maxFrequencies.put(map, max);
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

    private static String sanitize(String value) {
        if (value == null || value.equals("Null")) {
            return null;
        }
        return value;
    }

    private static void fillDict(String value, ArrayList<String> dict) {
        if (value == null) {
            return;
        }
        int pos = dict.indexOf(value);
        if (pos == -1) {
            dict.add(value);
        }
    }

    private static void fillDictFromJson(String file, ArrayList<String> dict) throws IOException {
        Gson gson = new Gson();
        FileReader reader = new FileReader(new File(file));
        String[] values = gson.fromJson(reader, String[].class);
        reader.close();
        for (String value : values) {
            dict.add(value);
        }
    }

    private void fillDictionary(CSVParser parser) {
        boolean warningShown = false;
        for (CSVRecord record : parser) {
            if (record.size() <= 6) {
                if (!warningShown) {
                    logger.warn("Additional features will not be added: dataset is corrupted or doesn't contain full feature set");
                    warningShown = true;
                }
                continue;
            }
            fillDict(sanitize(record.get(4)), langDictionary);
            fillDict(sanitize(record.get(6)), uriCategoryDictionary);
        }
        recalculateFeaturesNum();
    }

    private void fillDictionaryFromJson(String filePrefix) throws IOException {
        fillDictFromJson(filePrefix+".lang.json", langDictionary);
        fillDictFromJson(filePrefix+".uri.cat.json", uriCategoryDictionary);
        recalculateFeaturesNum();
    }

    private void dumpDictionariesToJson(String filePrefix) throws IOException {
        dumpDictionaryToJson(filePrefix+".lang.json", langDictionary);
        dumpDictionaryToJson(filePrefix+".uri.cat.json", uriCategoryDictionary);
    }

    private void dumpDictionaryToJson(String filename, ArrayList<String> dict) throws IOException {
        Gson gson = new Gson();
        FileWriter writer = new FileWriter(new File(filename));
        gson.toJson(dict.toArray(new String[0]), writer);
        writer.close();
    }

    private void recalculateFeaturesNum() {
        numUniqueFeatures = numSetRelatedFeatures + langDictionary.size() + uriCategoryDictionary.size();
        numFeatures = numUniqueFeatures + (numUniqueFeatures * (numUniqueFeatures - 1)) / 2;
    }

    private CSVParser open(File dataset) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(dataset), CSVFormat.DEFAULT.withDelimiter('\t'));
        if (parser.iterator().hasNext()) {
            parser.iterator().next();
        }
        return parser;
    }

    public void extractFeatures(File dataset, File output, boolean test) throws IOException {
        CSVParser parser;
        logger.info("Filling feature dictionary");
        if (!test) {
            parser = open(dataset);
            fillDictionary(parser);
            parser.close();
            logger.info("Dumping dictionaries");
            dumpDictionariesToJson(dataset.getPath());
        } else {
            fillDictionaryFromJson(dataset.getPath());
        }
        parser = open(dataset);
        logger.info("Extracting features");

        FileWriter svmOutput = new FileWriter(output);

        boolean arff = output.getName().endsWith(".arff");
        
        if (arff) {
            svmOutput.write("@RELATION mentiontype\n\n");
            for (int i = 0; i < numFeatures; ++ i) {
                svmOutput.write("@ATTRIBUTE attr" + i + " NUMERIC\n");
            }
            svmOutput.write("@ATTRIBUTE class {Person,Organization,Null}\n\n@DATA\n");
        }
        
        boolean first = true;
        for (CSVRecord record : parser) {
            int label = getLabel(record.get(1));
            String fullName = null, description = null, lang = null, uri = null, uriCategory = null;
            if (record.size() > 6) {
                fullName = sanitize(record.get(2));
                description = sanitize(record.get(3));
                lang = sanitize(record.get(4));
                uri = sanitize(record.get(5));
                uriCategory = sanitize(record.get(6));
            } else {
                logger.info("Record with reduced feature set: "+record.get(0));
            }
            double[] features = getFeatures(record.get(0), fullName, description, lang, uri, uriCategory);

            if (first) {
                first = false;
            } else {
                svmOutput.write('\n');
            }
        
            if (arff) {
                svmOutput.write(Joiner.on(',').join(Doubles.asList(features)));
                svmOutput.write(",");
                svmOutput.write(label == 0 ? "Null" : label == 1 ? "Person" : "Organization");
            } else {
                svmOutput.write(String.valueOf(label));
                for (int i = 0; i < features.length; i++) {
                    if (features[i] == 0) {
                        continue;
                    }
                    svmOutput.write(" "+(i+1)+":"+features[i]);
                }
            }
        }
        parser.close();
        svmOutput.close();
    }

    private final int numSetRelatedFeatures = sets.size()*2 + 1; //Sets twice + if uri exists in a dictionary
    private final ArrayList<String> uriCategoryDictionary = new ArrayList<>();
    private final ArrayList<String> langDictionary = new ArrayList<>();
    private int numUniqueFeatures = 0;
    private int numFeatures = 0;
    private double[] getFeatures(
            String username,
            String fullName,
            String description,
            String lang,
            String uri,
            String uriCategory) {

        //Preprocessing
        int langPos = langDictionary.indexOf(lang);
        int uriCategoryPos = uriCategoryDictionary.indexOf(uriCategory);
        int uriExists = uri == null ? 0 : 1;

        //Feature array initialization
        double[] features = new double[numFeatures];
        Arrays.fill(features, 0);
        Collection<String> parts = breakUsername(username);

        //Log reporting
        double random = new Random().nextDouble();
        if (random < 0.4) {
            logger.info("Username: "+username+", name: "+(fullName == null ? "" : fullName));
            logger.info("  Username parts: "+String.join(", ", parts));
            logger.info("  Name parts: "+String.join(", ", parts));
        }

        //Filling array of features
        int featureId = 0;
        for (String part : parts) {
            int setId = 0;
            for (HashMap<String, Integer> set : sets.values()) {
                int max = maxFrequencies.getOrDefault(set, 0);
                if (set.containsKey(part)) {
                    if (max == 0) {
                        max = 1;
                    }
                    double score = (double) set.get(part)/max;
                    if (features[featureId+setId] < score) {
                        features[featureId+setId] = score;
                    }
                }
                setId++;
            }
        }
        featureId += sets.size();
        features[featureId] = uriExists;
        featureId++;
        if (langPos != -1) {
            features[featureId + langPos] = 1;
        }
        featureId += langDictionary.size();
        if (uriCategoryPos != -1) {
            features[featureId + uriCategoryPos] = 1;
        }
        featureId += uriCategoryDictionary.size();

        for (Map.Entry<String, HashMap<String, Integer>> set : sets.entrySet()) {
            int max = maxFrequencies.getOrDefault(set.getValue(), 0);
            for (String name : set.getValue().keySet()) {
                if (username.toLowerCase().contains(name)) {
                    if (random < 0.4) {
                        logger.info("  Matched["+set.getKey()+", freq: "+set.getValue().get(name)+"]: " + name);
                    }
                    double score = (double) set.getValue().get(name)/max;
                    if (features[featureId] < score) {
                        features[featureId] = score;
                    }
                }
            }
            featureId++;
        }

        //Combinations of features
        int idx = numUniqueFeatures;
        for (int i = 0; i < numUniqueFeatures; i++) {
            for (int j = i+1; j < numUniqueFeatures; j++) {
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
        script.extractFeatures(new File(config.dataset), new File(config.output), config.test);
    }

    private static class TrainConfiguration {
        String[] input;
        String dataset;
        String output;
        int freqThreshold = 40;
        boolean test = false;
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
        options.addOption(
            Option.builder().desc("Treat input as a test set (requires files from training phase to be present)")
                .longOpt("test").build()
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
            configuration.test = line.hasOption("test");
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
}
