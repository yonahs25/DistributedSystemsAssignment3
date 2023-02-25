package ass3;

import java.io.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class job2 {
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, LongPairWritable> {
    
        // create a variable to store the count
        private LongPairWritable count;
        // create a variable to store the file paths list copy
        private File pathsListCopy;
    
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // initialize count to 0, 1
            count = new LongPairWritable(0, 1);
            BufferedReader br = new BufferedReader(new FileReader("ass3Files/paths.txt"));
            java.nio.file.Path path = Paths.get("ass3Files");
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
            // create a new file for the paths list copy
            pathsListCopy = new File("ass3Files/pathsListCopy.txt");
            // write the contents of the paths list to the copy file
            BufferedWriter bw = new BufferedWriter(new FileWriter(pathsListCopy));
            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line + "\n");
            }
            bw.close();
            br.close();
        }
    
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long index = 0;
            boolean found = false;
            // split the input value by whitespace
            String[] parts = value.toString().split("\\s");
            // read the paths list copy file line by line
            Scanner scanner = new Scanner(pathsListCopy);
            while (scanner.hasNextLine()) {
                // check if the current line matches the second part of the input value
                if (parts[1].equals(scanner.nextLine())) {
                    found = true;
                    break;
                }
                else {
                    index++;
                }
            }
            scanner.close();
            // if the path was found in the paths list copy, emit the key and count
            if (found) {
                count.setFirst(index);
                context.write(new Text(parts[0]), count);
            }
        }
    }
    
    public static class Reducer2 extends Reducer<Text, LongPairWritable, Text, Text> {
        // Set containing the hypernyms to be tested
        private HashMap<String, Boolean> testSet;
        private final String HYPERNYM_LIST = "ass3Files/hypernym.txt";
        private final String NUM_OF_FEATURES_FILE = "ass3Files/numOfFeatures.txt";
        // Number of features and a stemmer for preprocessing the hypernym text
        private long numOfFeatures;
        private Stemmer stemmer;
        
    
        @Override
        public void setup(Context context) throws IOException {
            // Initialize the stemmer
            stemmer = new Stemmer();
            Scanner scanner;
            BufferedReader br;
            scanner = new Scanner(new FileReader(NUM_OF_FEATURES_FILE));
            br = new BufferedReader(new FileReader(HYPERNYM_LIST));
            numOfFeatures = scanner.nextInt();

            System.out.println("Number of features: " + numOfFeatures);
            System.out.println("@@@@@@@@@@@@@@@@@@");
            scanner.close();
            
            // Process the hypernym list and add it to the test set
            testSet = new HashMap<>();
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] pieces = line.split("\\s");
                stemmer.add(pieces[0].toCharArray(), pieces[0].length());
                stemmer.stem();
                pieces[0] = stemmer.toString();
                stemmer.add(pieces[1].toCharArray(), pieces[1].length());
                stemmer.stem();
                pieces[1] = stemmer.toString();
                testSet.put(pieces[0] + "$" + pieces[1], pieces[2].equals("True"));
            }
            br.close();
        }
    
        @Override
        public void reduce(Text key, Iterable<LongPairWritable> counts, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();
            
            // Only process the key if it exists in the test set
            if (testSet.containsKey(keyString)) {
                // Create a feature vector from the counts for this key
                long[] featuresVector = createFeaturesVector(counts);
                
                // Append the classification label to the end of the feature vector
                StringBuilder sb = new StringBuilder();
                for (long featureValue : featuresVector) {
                    sb.append(featureValue).append(",");
                }
                sb.append(testSet.get(keyString));
                
                // Write the key-value pair to the context
                context.write(key, new Text(sb.toString()));
            }
        }

        /**
         * Creates a feature vector from the counts for a given key.
         * @param counts An iterable collection of LongPairWritable objects.
         * @return A long array representing the feature vector.
         */
        private long[] createFeaturesVector(Iterable<LongPairWritable> counts) {
            long[] featuresVector = new long[(int) numOfFeatures];
            for (LongPairWritable count : counts) {
                int featureIndex = (int) count.getFirst();
                long featureValue = count.getSecond();
                featuresVector[featureIndex] += featureValue;
            }
            return featuresVector;
        }
    }
    
}