package com.bigdatatools;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.CreateMode;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.Telemetry;
import org.apache.commons.cli.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static com.tableau.hyperapi.Sql.escapeStringLiteral;

public class CSVToHyperConverter {

    private static String tableName;
    private static String inputCsv;
    private static String inputSchema;
    private static String outputHyper;

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) throws IOException {

        parseCommandLineOptions(args);

        Path customerDatabasePath = Paths.get(outputHyper);
        String[] columns = getHeaderLine(inputCsv).split(",");
        TableDefinition tableDefinition = HyperTableSchemaBuilder.getHyperTableDefinition(tableName,  Paths.get(inputSchema), Arrays.asList(columns));
        Map<String, String> processParameters = new HashMap<>();
        // Limits the number of Hyper event log files to two.
        processParameters.put("log_file_max_count", "2");
        // Limits the size of Hyper event log files to 100 megabytes.
        processParameters.put("log_file_size_limit", "100M");

        try (HyperProcess process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "CSVToHyperConverter", processParameters)) {
            Map<String, String> connectionParameters = new HashMap<>();
            connectionParameters.put("lc_time", "en_US");

            try (Connection connection = new Connection(process.getEndpoint(),
                    customerDatabasePath.toString(),
                    CreateMode.CREATE_AND_REPLACE,
                    connectionParameters)) {

                connection.getCatalog().createTable(tableDefinition);

                Path customerCSVPath = resolveInputFile(inputCsv);
                Date processBeginDate = new Date();
                System.out.println(processBeginDate.toString() + " - CSV To Hyper table conversion begins for " + inputCsv + "\n");
                long countInTable = connection.executeCommand(
                        "COPY " + tableDefinition.getTableName() +
                                " FROM " + escapeStringLiteral(customerCSVPath.toString()) +
                                " WITH (format csv, NULL 'NULL', delimiter ',', header)"
                ).getAsLong();
                Date processEndDate = new Date();
                System.out.println(processEndDate.toString() + " - Conversion completed, time taken is "+ String.valueOf((processEndDate.getTime() -processBeginDate.getTime())/1000) + " seconds. The number of rows in table " + tableDefinition.getTableName() + " is " + countInTable + "\n");
            }
            System.out.println("The connection to the Hyper file has been closed");
        }
        System.out.println("The Hyper process has been shut down");
    }

    private static Path resolveInputFile(String filename) {
        for (Path path = Paths.get(".").toAbsolutePath(); path != null; path = path.getParent()) {
            Path file = path.resolve(filename);
            if (Files.isRegularFile(file)) {
                return file;
            }
        }
        throw new IllegalAccessError("Could not find input file. Check the working directory." + filename);
    }


    public static String getHeaderLine(String inputCsv) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(inputCsv));
        String line = br.readLine();
        br.close();
        return line;
    }


    private static void parseCommandLineOptions (String[] args) {

        Options options = new Options();
        Option tableOption = new Option("t", "tableName", true, "Table name ");
        tableOption.setRequired(true);
        options.addOption(tableOption);

        Option inputCsvOption = new Option("i", "inputCsv", true, "Input CSV file path");
        inputCsvOption.setRequired(true);
        options.addOption(inputCsvOption);

        Option inputSchemaOption = new Option("s", "jsonSchema", true, "Input JSON Schema file path ");
        inputSchemaOption.setRequired(true);
        options.addOption(inputSchemaOption);

        Option outputHyperOption = new Option("o", "outputHyperFile", true, "Output Hyper File");
        outputHyperOption.setRequired(true);
        options.addOption(outputHyperOption);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            tableName = cmd.getOptionValue("tableName");
            inputCsv = cmd.getOptionValue("inputCsv");
            inputSchema = cmd.getOptionValue("jsonSchema");
            outputHyper = cmd.getOptionValue("outputHyperFile");

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("CSVToHyperConverter", options);
            System.exit(1);
        }

    }

}
