package eu.fbk.microneel.util;

import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Simple script to combine the results of classification from liblinear
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PersonOrgProduceResult {

    private static final Logger logger = LoggerFactory.getLogger(PersonOrgProduceResult.class);

    public void run(TrainConfiguration config) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(new File(config.input)), CSVFormat.DEFAULT.withDelimiter('\t'));
        //Skipping header
        if (parser.iterator().hasNext()) {
            parser.iterator().next();
        }

        LineNumberReader reader = new LineNumberReader(new FileReader(new File(config.results)));
        FileWriter output = new FileWriter(config.output);
        boolean first = true;
        for (CSVRecord record : parser) {
            String username = record.get(0);
            String rawLabel = reader.readLine();
            if (rawLabel == null) {
                logger.error("Inconsistent input dataset and the results file");
                output.close();
                reader.close();
                parser.close();
                return;
            }

            if (first) {
                first = false;
            } else {
                output.write('\n');
            }
            output.write(username + "\t" + getLabel(rawLabel));
        }
        parser.close();
        reader.close();
        output.close();
    }

    private String getLabel(String rawLabel) {
        switch (rawLabel.trim()) {
        default:
        case "0":
            return "Null";
        case "1":
            return "Person";
        case "2":
            return "Organization";
        }
    }

    public static void main(String[] args) throws IOException {
        TrainConfiguration config = loadTrainConfig(args);
        if (config == null) {
            logger.error("Unable to read configuration");
            return;
        }

        PersonOrgProduceResult script = new PersonOrgProduceResult();
        script.run(config);
    }

    private static class TrainConfiguration {

        String input;
        String results;
        String output;
    }

    private static TrainConfiguration loadTrainConfig(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("o").desc("Output file with features")
                        .required().hasArg().argName("file").longOpt("output").build()
        );
        options.addOption(
                Option.builder("i").desc("Input dataset")
                        .required().hasArg().argName("file").longOpt("input").build()
        );
        options.addOption(
                Option.builder("r").desc("Classification results")
                        .required().hasArg().argName("results").longOpt("results").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            TrainConfiguration configuration = new TrainConfiguration();
            configuration.output = line.getOptionValue("output");
            configuration.input = line.getOptionValue("input");
            configuration.results = line.getOptionValue("results");
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
                "java -Dfile.encoding=UTF-8 " + PersonOrgClassifier.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }
}
