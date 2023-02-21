package ass3;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;

public class job2 {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, WritableLongPair> {

        private WritableLongPair count;
        private File pathsListCopy;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            count = new WritableLongPair(0, 1);
            // boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            boolean local = false;
            BufferedReader br;
            if (local) {
                br = new BufferedReader(new FileReader("ass3Files/paths.txt"));
            } else {
                AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                S3Object object = s3.getObject(new GetObjectRequest("yo-ass3-output-bucket", "ass3Files/paths.txt"));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            java.nio.file.Path path = Paths.get("ass3Files");
            if (!Files.exists(path))
                Files.createDirectory(path);
            pathsListCopy = new File("ass3Files/pathsListCopy.txt");
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
            String[] parts = value.toString().split("\\s");
            Scanner scanner = new Scanner(pathsListCopy);
            while (scanner.hasNextLine()) {
                if (parts[1].equals(scanner.nextLine())) {
                    found = true;
                    break;
                }
                else
                    index++;
            }
            if (found) {
                count.setL1(index);
                context.write(new Text(parts[0]), count);
            }
            scanner.close();
        }
    }

    public static class Reducer2 extends Reducer<Text, WritableLongPair, Text, Text> {
        private HashMap<String, Boolean> testSet;
        private final String BUCKET = "yo-ass3-output-bucket";
        private final String HYPERNYM_LIST = "ass3Files/hypernym.txt";
        private final String NUM_OF_FEATURES_FILE = "ass3Files/numOfFeatures.txt";
        private long numOfFeatures;
        private Stemmer stemmer;
        private AmazonS3 s3;

        @Override
        public void setup(Context context) throws IOException {
            stemmer = new Stemmer();
            Scanner scanner;
            boolean local = false;
            BufferedReader br;
            if (local) {
                scanner = new Scanner(new FileReader(NUM_OF_FEATURES_FILE));
                br = new BufferedReader(new FileReader(HYPERNYM_LIST));
            } else {
                s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                System.out.print("Downloading no. of features file from S3... ");
                S3Object object = s3.getObject(new GetObjectRequest("yo-ass3-output-bucket", NUM_OF_FEATURES_FILE));
                System.out.println("Done.");
                scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
                object = s3.getObject(new GetObjectRequest(BUCKET, HYPERNYM_LIST));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            numOfFeatures = scanner.nextInt();
            System.out.println("Number of features: " + numOfFeatures);
            scanner.close();
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
        public void reduce(Text key, Iterable<WritableLongPair> counts, Context context) throws IOException, InterruptedException {
            String keyAsString = key.toString();
            if (testSet.containsKey(keyAsString)) {
                long[] featuresVector = new long[(int) numOfFeatures];
                for (WritableLongPair count : counts) {
                    featuresVector[(int) count.getL1()] += count.getL2();
                }
                StringBuilder sb = new StringBuilder();
                for (long index : featuresVector)
                    sb.append(index).append(",");
                sb.append(testSet.get(keyAsString));
                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                System.out.println(key.toString());
                System.out.println(sb.toString());
                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static class Mapper22 extends Mapper<LongWritable, Text, Text, WritableLongPair> {
    
        // create a variable to store the count
        private WritableLongPair count;
        // create a variable to store the file paths list copy
        private File pathsListCopy;
    
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // initialize count to 0, 1
            count = new WritableLongPair(0, 1);
            // determine whether the job is running locally or on EMR
            boolean isLocal = false;
            // boolean isLocal = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            BufferedReader br;
            // if running locally, read the file from the local file system
            if (isLocal) {
                br = new BufferedReader(new FileReader("ass3Files/paths.txt"));
            }
            // if running on EMR, read the file from Amazon S3
            else {
                // create an S3 client
                AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                // get the file from S3
                S3Object object = s3.getObject(new GetObjectRequest("yo-ass3-output-bucket", "ass3Files/paths.txt"));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            // create the directory if it does not exist
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
                count.setL1(index);
                context.write(new Text(parts[0]), count);
            }
        }
    }
    
    public static class Reducer22 extends Reducer<Text, WritableLongPair, Text, Text> {
        // Set containing the hypernyms to be tested
        private HashMap<String, Boolean> testSet;
        
        // S3 bucket names
        private final String BUCKET = "yo-ass3-output-bucket";
        private final String HYPERNYM_LIST = "ass3Files/hypernym.txt";
        private final String NUM_OF_FEATURES_FILE = "ass3Files/numOfFeatures.txt";
        
        // Number of features and a stemmer for preprocessing the hypernym text
        private long numOfFeatures;
        private Stemmer stemmer;
        
        // AmazonS3 client for downloading hypernym and feature data from S3
        private AmazonS3 s3;
    
        @Override
        public void setup(Context context) throws IOException {
            // Initialize the stemmer
            stemmer = new Stemmer();
            
            // Create file readers and scanners depending on whether this is running locally or on S3
            Scanner scanner;
            BufferedReader br;
            boolean local = false;
            if (local) {
                scanner = new Scanner(new FileReader(NUM_OF_FEATURES_FILE));
                br = new BufferedReader(new FileReader(HYPERNYM_LIST));
            } else {
                // Initialize the S3 client
                s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                
                // Download the number of features file from S3
                System.out.print("Downloading no. of features file from S3... ");
                S3Object object = s3.getObject(new GetObjectRequest(BUCKET, NUM_OF_FEATURES_FILE));
                System.out.println("Done.");
                
                // Read the number of features from the downloaded file
                scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
                numOfFeatures = scanner.nextInt();
                System.out.println("Number of features: " + numOfFeatures);
                
                // Download the hypernym list from S3
                object = s3.getObject(new GetObjectRequest(BUCKET, HYPERNYM_LIST));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            
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
        public void reduce(Text key, Iterable<WritableLongPair> counts, Context context) throws IOException, InterruptedException {
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
         * @param counts An iterable collection of WritableLongPair objects.
         * @return A long array representing the feature vector.
         */
        private long[] createFeaturesVector(Iterable<WritableLongPair> counts) {
            long[] featuresVector = new long[(int) numOfFeatures];
            for (WritableLongPair count : counts) {
                int featureIndex = (int) count.getL1();
                long featureValue = count.getL2();
                featuresVector[featureIndex] += featureValue;
            }
            return featuresVector;
        }
    }
    
}