package bd.ac.buet.cse.ms.thesis.legacy.utils;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import static bd.ac.buet.cse.ms.thesis.legacy.Main.INPUT_FILE_NAMES;
import static bd.ac.buet.cse.ms.thesis.legacy.Main.INPUT_FILE_PATH;

public class Tsv2Csv {

    public static void main(String[] args) throws IOException {
        TsvParserSettings settings = new TsvParserSettings();
        settings.setMaxCharsPerColumn(999999);
        TsvParser parser = new TsvParser(settings);

        CsvWriterSettings writerSettings = new CsvWriterSettings();
        writerSettings.setMaxCharsPerColumn(999999);
        writerSettings.setQuoteAllFields(true);
        CsvWriter writer = new CsvWriter(new FileWriter(INPUT_FILE_PATH + "data1.csv"), writerSettings);

        long start = System.currentTimeMillis();

        long rows = 0;

        for (String inputFileName : INPUT_FILE_NAMES) {
            long fileRows = 0;

            long innerStart = System.currentTimeMillis();

            int idx = -1;
            for (String[] row : parser.iterate(new FileReader(INPUT_FILE_PATH + inputFileName))){
                idx++;
                if (idx == 0) {
                    // header row
                    continue;
                }

                String[] values = new String[15];
                for (int i = 0; i < values.length; i++) {
                    values[i] = i < row.length ? row[i] : null;
                }

                writer.writeRow(values[0], values[1], values[2], values[3], values[4],
                        values[5] != null ? values[5].replace("\"", "").replace("\\", "") : null,
                        values[6],
                        values[7] != null ? Integer.parseInt(values[7]) : null,
                        values[8] != null ? Integer.parseInt(values[8]) : null,
                        values[9] != null ? Integer.parseInt(values[9]) : null,
                        values[10],
                        values[11],
                        values[12] != null ? values[12].replace("\"", "").replace("\\", "") : null,
                        values[13] != null ? values[13].replace("\"", "").replace("\\", "") : null,
                        values[14]);

                fileRows++;
            }

            long innerEnd = System.currentTimeMillis();

            rows += fileRows;

            System.out.println(inputFileName + " Rows: " + fileRows + ", duration: " + ((innerEnd - innerStart) / 1000.0) + " seconds.");
        }

        long end = System.currentTimeMillis();

        double duration = (end - start) / 1000.0;

        System.out.println("Total Rows: " + rows + ", duration: " + duration + " seconds");

        parser.stopParsing();
        writer.close();
    }
}
