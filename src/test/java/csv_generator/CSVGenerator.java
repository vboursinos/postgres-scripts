package csv_generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CSVGenerator {

    public static void main(String[] args) {
        List<CSVInfo> csvInfoList = List.of(
                new CSVInfo("src/main/resources/csv/tab_c_gt.csv", "src/main/resources/big-csv/tab_c_gt.csv"),
                new CSVInfo("src/main/resources/csv/tab_oab.csv", "src/main/resources/big-csv/tab_oab.csv"),
                new CSVInfo("src/main/resources/csv/tab_sbfa.csv", "src/main/resources/big-csv/tab_sbfa.csv"),
                new CSVInfo("src/main/resources/csv/tab_t_5_c_c_1.csv", "src/main/resources/big-csv/tab_t_5_c_c_1.csv")
        );
        int numberOfRows = 80000;
        for (CSVInfo csvInfo : csvInfoList) {
            try {
                processCSV(csvInfo.getInputFile(), csvInfo.getOutputFile(), numberOfRows);
                System.out.println("New CSV file created successfully.");
            } catch (IOException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }
        }
    }

    public static void processCSV(String inputFile, String outputFile, int numberOfRows) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        String line;
        int rowCount = 0;
        boolean isFirstIteration = true;

        String headers = reader.readLine();
        writer.write(headers);
        writer.newLine();

        while (rowCount < numberOfRows) {
            if (!isFirstIteration) {
                reader.close();
                reader = new BufferedReader(new FileReader(inputFile));
                reader.readLine();
            }

            isFirstIteration = false;

            while ((line = reader.readLine()) != null && rowCount < numberOfRows) {
                writer.write(line);
                writer.newLine();
                rowCount++;
            }
        }

        reader.close();
        writer.close();
    }
}
