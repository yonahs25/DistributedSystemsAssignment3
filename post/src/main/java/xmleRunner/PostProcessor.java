package xmleRunner;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;


public class PostProcessor {

    private static final String PREFIX1 = "@RELATION nounpair\n\n";
    private static final String PREFIX2 = "@RELATION nounpair\n\n@ATTRIBUTE nounPair STRING\n";
    private static final String POSTFIX = "@ATTRIBUTE ans {true, false}\n\n@DATA\n";

    public static void main(String[] args) throws IOException {
        // Create directory for output files if it does not exist
        java.nio.file.Path path = Paths.get("classifier_input");
        if (!Files.exists(path))
            Files.createDirectory(path);

        // Initialize readers and writers for input/output files
        BufferedReader br1, br2;
        BufferedWriter bw1, bw2 = null;
        br1 = new BufferedReader(new FileReader("output/part-r-00000"));
        br2 = new BufferedReader(new FileReader("ass3Files/numOfFeatures.txt"));
        bw1 = new BufferedWriter(new FileWriter(new File("classifier_input/processed_single_corpus.arff")));
        bw2 = new BufferedWriter(new FileWriter(new File("classifier_input/processed_single_corpus_with_words.arff")));

        // Read the number of features from the numOfFeatures.txt file and print it
        int vectorLength = Integer.parseInt(br2.readLine());
        System.out.println("Number of features: " + vectorLength);

        // Write the prefix for the ARFF files
        bw1.write(PREFIX1);
        bw2.write(PREFIX2);

        // Add the feature attributes to the ARFF files
        for (int i = 0; i < vectorLength; i++) {
            bw1.write("@ATTRIBUTE p" + i + " REAL\n");
            bw2.write("@ATTRIBUTE p" + i + " REAL\n");
        }

        // Write the postfix for the ARFF files
        bw1.write(POSTFIX);
        bw2.write(POSTFIX);

        // Populate the ARFF files with actual data
        String line;
        while ((line = br1.readLine()) != null) {
            bw1.write(line.substring(line.indexOf("\t") + 1) + "\n");
            bw2.write(line + "\n");
        }

        // Close the input/output files
        br1.close();
        br2.close();
        bw1.close();
        bw2.close();
    }
}