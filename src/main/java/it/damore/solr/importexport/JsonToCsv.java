package it.damore.solr.importexport;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.util.*;

import org.json.*;

public class JsonToCsv {

    public static void main(String[] args) throws IOException, JSONException {
        String inputFilePath = "/path/to/large.json";
        String outputFilePath = "/path/to/output.csv";

        // Set up character set and delimiter for CSV output
        Charset charset = Charset.forName("UTF-8");
        String delimiter = ",";

        // Set up JSON reader for input file
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));

        // Set up memory-mapped file for output
        RandomAccessFile outputFile = new RandomAccessFile(outputFilePath, "rw");
        FileChannel outputChannel = outputFile.getChannel();
        MappedByteBuffer outputBuffer = outputChannel.map(FileChannel.MapMode.READ_WRITE, 0, outputFile.length());

        // Process JSON input and write CSV output
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
            lineNumber++;

            // Parse JSON line
            JSONObject jsonObject = new JSONObject(line);

            // Write CSV line to output buffer
            String csvLine = getCsvLine(jsonObject, delimiter);
            outputBuffer.put(charset.encode(csvLine + "\n"));
        }

        // Clean up resources
        reader.close();
        outputChannel.close();
        outputFile.close();
    }

    private static String getCsvLine(JSONObject jsonObject, String delimiter) throws JSONException {
        // Define CSV field order and default values
        List<String> fields = Arrays.asList("company_complete", "partyACompanyCount", "partyBCompanyCount", "relationCompanyCount", "type", "zhaobiaoCount", "zhongbiaoCount");
        Map<String, String> defaultValues = new HashMap<String, String>();
        defaultValues.put("partyACompanyCount", "0");
        defaultValues.put("partyBCompanyCount", "0");
        defaultValues.put("relationCompanyCount", "0");
        defaultValues.put("type", "0");
        defaultValues.put("zhaobiaoCount", "0");
        defaultValues.put("zhongbiaoCount", "0");

        // Build CSV line from JSON object
        StringBuilder csvLineBuilder = new StringBuilder();
        for (String field : fields) {
            String value = jsonObject.optString(field, defaultValues.get(field));
            csvLineBuilder.append(value).append(delimiter);
        }

        // Remove trailing delimiter and return CSV line
        return csvLineBuilder.deleteCharAt(csvLineBuilder.length() - 1).toString();
    }

}
