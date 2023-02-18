package ass3;
// // aws imports
// import com.amazonaws.regions.Regions;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.GetObjectRequest;
// import com.amazonaws.services.s3.model.S3Object;
// // hadoop imports
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// // java imports
// import java.io.*;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.HashMap;

// public class job2 {
//     private static final String BUCKET = "yo-ass3-output-bucket";
//     private static final String PATHS_LIST = "ass3Files/dependencyPaths.txt";
//     private static final String NUM_OF_FEATURES_FILE = "ass3Files/numOfFeatures.txt";
//     private static final String HYPERNYM_LIST = "ass3Files/hypernym.txt";

//     public static class Mapper2 extends Mapper<LongWritable, Text, Text, LongPairWritable> {
//         private LongPairWritable count;
//         private File pathsListCopy;

//         @Override
//         public void setup(Context context) throws IOException, InterruptedException {
//             // Read the paths list from S3.
//             // Create a copy of the paths list in the local file system.
//             // This is done in order to avoid reading the paths list from S3 for every mapper.
//             // The copy of the paths list is used by the findIndexInPathsList method.
//             // The copy of the paths list is deleted in the cleanup method.
//             count = new LongPairWritable(0, 1);
//             boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
//             BufferedReader br;
//             if (local) {
//                 br = new BufferedReader(new FileReader(PATHS_LIST));
//             } else {
//                 // Create an instance of Amazon S3 client
//                 AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
//                 // Read the object from S3 and convert its contents to a stream of bytes
//                 S3Object object = s3.getObject(new GetObjectRequest(BUCKET, PATHS_LIST));
//                 InputStream inputStream = object.getObjectContent();
//                 br = new BufferedReader(new InputStreamReader(inputStream));
//             }
//             java.nio.file.Path path = Paths.get("ass3Files");
//             if (!Files.exists(path)) {
//                 Files.createDirectory(path);
//             }
//             pathsListCopy = new File("ass3Files/pathsListCopy.txt");
//             BufferedWriter bw = new BufferedWriter(new FileWriter(pathsListCopy));
//             String line;
//             while ((line = br.readLine()) != null) {
//                 bw.write(line + "\n");
//             }
//             bw.close();
//             br.close();
//         }


//         @Override
//         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//             // The mapper receives a line from the output of the previous phase.
//             // The line is in the following format: <word> <path> <count>
//             // The mapper extracts the word and the path from the line.
//             // The mapper finds the index of the path in the paths list.
//             // The mapper emits the word and a pair of the index and the count.
//             String[] parts = value.toString().split("\\s");
//             int index = utils.findIndexInPathsList(parts[1], pathsListCopy);
//             if (index != -1) {
//                 count.setFirst(index);
//                 context.write(new Text(parts[0]), count);
//             }
//         }

        
//     }

//     public static class Reducer2 extends Reducer<Text, LongPairWritable, Text, Text> {

//     private HashMap<String, Boolean> testSet;
//     private final String BUCKET = "";
//     private final String HYPERNYM_LIST = "ass3Files/hypernym.txt";
//     private final String NUM_OF_FEATURES_FILE = "ass3Files/numOfFeatures.txt";
//     private long numOfFeatures;

//     @Override
//     protected void setup(Context context) throws IOException, InterruptedException {
//         // Read the dictionary file from S3.
//         // Create a copy of the dictionary file in the local file system.
//         // This is done in order to avoid reading the dictionary file from S3 for every reducer.
//         // The copy of the dictionary file is used by the loadDictionary method.
//         // The copy of the dictionary file is deleted in the cleanup method.
        
//         // Get the bucket and dictionary file name from the configuration
//         String bucketName = "yo-ass3-output-bucket";
//         String dictionaryFileName = "ass3Files/dictionary.txt";

//         // Create an S3 client and get the dictionary file object
//         AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
//         S3Object object = s3.getObject(new GetObjectRequest(bucketName, dictionaryFileName));

//         // Read the dictionary file into a BufferedReader
//         BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

//         // Create a local copy of the dictionary file
//         Path dictionaryCopyPath = Paths.get("dictionary_copy.txt");
//         if (!Files.exists(dictionaryCopyPath)) {
//             Files.createFile(dictionaryCopyPath);
//         }
//         BufferedWriter bw = new BufferedWriter(new FileWriter(dictionaryCopyPath.toFile()));

//         // Copy the contents of the dictionary file to the local copy
//         String line;
//         while ((line = br.readLine()) != null) {
//             bw.write(line + "\n");
//         }

//         // Close the BufferedReader and BufferedWriter
//         br.close();
//         bw.close();
//     }


//     @Override
//     public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
//         HashMap<Long, Long> features = new HashMap<>();
//         long count = 0;
        
//         for (LongPairWritable value : values) {
//             long index = value.getFirst();
//             long val = value.getSecond();
//             utils.updateFeaturesMap(features, index, val);
//             count += val;
//         }
    
//         if (count > 10) { // filter
//             StringBuilder vector = utils.createVectorFromFeatures(features, numOfFeatures, key.toString(), testSet);
//             writeVectorToContext(context, key, vector.toString());
//         }
//     }
    
//     /**
//      * Writes the given vector string to the context, using the given key to determine whether
//      * the output should be written to the training or test set.
//      */
//     private void writeVectorToContext(Context context, Text key, String vector) throws IOException, InterruptedException {
//         if (vector.charAt(0) == 't') {
//             context.write(new Text("test"), new Text(vector.substring(5)));
//         } else {
//             context.write(new Text("train"), new Text(vector.substring(6)));
//         }
//     }    
// }
// }



///Phase2
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

/**
 * Created by asafchelouche on 26/7/16.
 */

public class job2 {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, WritableLongPair> {

        private WritableLongPair count;
        private File pathsListCopy;

        /**
         * Setup a Mapper node. Copies the list of dependency paths from the S3 bucket to the local file system.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * Checks for a dependency path's index the dependency paths file. Write to context the noun pair, the dependency
         * path's index, and a count of 1.
         * @param key a noun pair.
         * @param value a dependency path.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * Setup a Reducer node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            stemmer = new Stemmer();
            // boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
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

        /**
         *
         * @param key a noun pair.
         * @param counts a list of WritableLongPair, each one being an index of a dependency path and a count of its
         *               appearances, respectively.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
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

    /**
     * Main method for this Map-Reduce step. Processes the noun pairs and their dependency paths into a file which
     * contains the pairs and their features vector. This file would afterwards be processed with PostProcessor.java
     * into an .arff file for use by WEKA.
     * @param args an array of 2 Strings: input path, output path.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IOException("Phase 2: supply 3 arguments");
        Configuration conf = new Configuration();
        conf.set("LOCAL_OR_EMR", String.valueOf(args[2].equals("local")));
        Job job = Job.getInstance(conf, "Phase 2");
        job.setJarByClass(job2.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WritableLongPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 2 - input path: " + args[0] + ", output path: " + args[1]);
        if (job.waitForCompletion(true))
            System.out.println("Phase 2: job completed successfully");
        else
            System.out.println("Phase 2: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Number of key-value pairs sent to reducers in phase 2: " + counter.getValue());
    }

}