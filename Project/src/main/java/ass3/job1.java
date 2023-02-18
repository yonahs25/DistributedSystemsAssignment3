package ass3;
// //AWS imports
// import com.amazonaws.AmazonClientException;
// import com.amazonaws.AmazonServiceException;
// import com.amazonaws.regions.Region;
// import com.amazonaws.regions.Regions;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3Client;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.PutObjectRequest;
// //Hadopp imports
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Counter;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// //Java imports
// import java.io.BufferedWriter;
// import java.io.File;
// import java.io.FileWriter;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Paths;
// import java.util.HashSet;

// import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

// // Phase 1: Find all shortest dependency paths between nouns in a sentence.
// public class job1 {
//     public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
//         @Override
//         public void setup(Context context) throws IOException, InterruptedException {
//         }

//         // Input: <key, value> = <line number, ngram>
//         // Output: <key, value> = <dependency path, noun pair>
//         @Override
//         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//             // Split the ngram into its components. 
//             // The first component is the ngram's frequency.
//             // The second component is the ngram itself.
//             String[] line = value.toString().split("\t");
//             String[] splitted = line[1].split(" ");
//             Node[] nodes = utils.createNodes(splitted); // Get the nodes of the ngram.
//             if (nodes == null)
//                 return;
//             Node root = utils.constructParsedTree(nodes);
//             searchDependencyPath(root, "", root, context);
//         }

//         // Recursively search for dependency paths between nouns in the ngram.
//         // The method is called recursively on each node in the ngram.
//         private void searchDependencyPath(Node currentNode, String accumulatedPath, Node pathStartNode, Context context) throws IOException, InterruptedException {
//             if (currentNode.isNoun() && accumulatedPath.isEmpty()) {
//                 // If the current node is a noun and we haven't accumulated any path yet,
//                 // explore its children nodes recursively and add their dependency path
//                 // components to the accumulated path
//                 for (Node childNode : currentNode.getChildren()) {
//                     searchDependencyPath(childNode, currentNode.getDependencyPathComponent(), currentNode, context);
//                 }
//             } else if (currentNode.isNoun()) {
//                 // If the current node is a noun and we have accumulated a path so far,
//                 // construct the final path string and write it to the context
//                 String finalPathString = accumulatedPath + ":" + currentNode.getDependencyPathComponent();
//                 Text pathText = new Text(finalPathString);
//                 Text biarcText = new Text(pathStartNode.getStemmedWord() + "$" + currentNode.getStemmedWord());
//                 context.write(pathText, biarcText);

//                 // Recurse on the current node to explore its children nodes
//                 searchDependencyPath(currentNode, "", currentNode, context);
//             } else if (!accumulatedPath.isEmpty()) {
//                 // If the current node is not a noun, but we have accumulated a path so far,
//                 // explore its children nodes recursively and add their dependency path
//                 // components to the accumulated path
//                 for (Node childNode : currentNode.getChildren()) {
//                     searchDependencyPath(childNode, accumulatedPath + ":" + currentNode.getDependencyPathComponent(), pathStartNode, context);
//                 }
//             }
//         }

//     }

//     public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

//         private File pathsFile; // The local file to write the dependency paths to
//         private BufferedWriter bw; // The buffered writer to write to the local file
//         private long numOfFeatures; // The number of dependency paths that are treated as features
//         private final String BUCKET_NAME = "yo-ass3-output-bucket"; // The name of the S3 bucket to upload the local file to
//         private boolean local; // Whether the job is running locally or on EMR
//         private int DPmin; // The minimum number of noun pairs that a dependency path must appear in to be treated as a feature

//         /**
//          * Setup up a Reducer node.
//          * @param context the Map-Reduce job context.
//          * @throws IOException
//          */
//         @Override
//         public void setup(Context context) throws IOException {
//             java.nio.file.Path path = Paths.get("ass3Files");
//             if (!Files.exists(path))
//                 Files.createDirectory(path);
//             pathsFile = new File("ass3Files/dependencyPaths.txt");
//             bw = new BufferedWriter(new FileWriter(pathsFile));
//             numOfFeatures = 0;
//             local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
//             DPmin = Integer.parseInt(context.getConfiguration().get("DPMIN"));
//             System.out.println("Reducer: DPmin is set to " + DPmin);
//         }

//         /**
//          * Counts how many noun pairs exist for a particular dependency path. If there are more than DPmin, this
//          * dependency path would be treated as a feature, thus taking an index in the features vector for each noun pair
//          * in the corpus.
//          * Each dependency path that is counted as a feature would be written to a local file.
//          * @param key a dependency path.
//          * @param nounPairs all pairs of nouns which appear in this dependency path in the corpus.
//          * @param context the Map-Reduce job context.
//          * @throws IOException
//          * @throws InterruptedException
//          */
//         @Override
//         public void reduce(Text key, Iterable<Text> nounPairs, Context context) throws IOException, InterruptedException {
//             HashSet<Text> set = new HashSet<>(DPmin);

//             for (Text nounPair : nounPairs) {
//                 // If the set already contains DPmin unique noun pairs, stop adding more
//                 if (set.size() == DPmin) {
//                     break;
//                 }// If the set already contains the current noun pair, skip it
//                 else
//                 if (set.contains(nounPair)) {
//                     continue;
//                 }// Otherwise, add the current noun pair to the set
//                 set.add(nounPair);
//             }
//             // If the set has at least DPmin unique noun pairs, write the key to the paths file
//             if (set.size() >= DPmin) {
//                 bw.write(key.toString() + "\n");
//                 numOfFeatures++;
//                 // Emit each unique noun pair with the current key
//                 for (Text nounPair : set) {
//                     context.write(nounPair, key);
//                 }
//             }
//         }


//         /**
//          * Writes 2 files to an S3 bucket:
//          * 1. the local file, containing all dependency paths that are counted as features
//          * 2. a file which contains the number of features, i.e. the number of dependency paths with more than DPmin noun pairs
//          * @throws IOException
//          */
//         @Override
//         public void cleanup(Context context) throws IOException {
//             File numOfFeaturesFile = new File("ass3Files/numOfFeatures.txt");
//             System.out.println("Features vector length: " + numOfFeatures);
//             writeNumOfFeaturesFile(numOfFeaturesFile);
//             closeWriters();
//             if (local) {
//                 copyPathsListToLocal();
//             } else {
//                 uploadFilesToS3(numOfFeaturesFile);
//             }
//         }

//         private void writeNumOfFeaturesFile(File numOfFeaturesFile) throws IOException {
            
//             try (BufferedWriter bw = new BufferedWriter(new FileWriter(numOfFeaturesFile))) {
//                 bw.write(numOfFeatures + "\n");
//             }
//         }

//         private void closeWriters() throws IOException {
//             bw.close();
//         }

//         private void copyPathsListToLocal() throws IOException {
//             Files.copy(new File("ass3Files/dependencyPaths.txt").toPath(), new File("ass3Files/pathsListCopy.txt").toPath(), REPLACE_EXISTING);
//         }

//         private void uploadFilesToS3(File numOfFeaturesFile) {
//             AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
//             Region usEast1 = Region.getRegion(Regions.US_EAST_1);
//             s3.setRegion(usEast1);
//             try {
//                 uploadDependencyPathsFileToS3(s3);
//                 uploadNumOfFeaturesFileToS3(s3, numOfFeaturesFile);
//             } catch (AmazonServiceException ase) {
//                 handleAmazonServiceException(ase);
//             } catch (AmazonClientException ace) {
//                 handleAmazonClientException(ace);
//             }
//         }

//         private void uploadDependencyPathsFileToS3(AmazonS3 s3) {
//             System.out.print("Uploading the dependency paths file to S3... ");
//             s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/dependencyPaths.txt", pathsFile));
//             System.out.println("Done.");
//         }

//         private void uploadNumOfFeaturesFileToS3(AmazonS3 s3, File numOfFeaturesFile) {
//             System.out.print("Uploading num of features file to S3... ");
//             s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/numOfFeatures.txt", numOfFeaturesFile));
//             System.out.println("Done.");
//         }

//         private void handleAmazonServiceException(AmazonServiceException ase) {
//             // Capture some information about the error that occurred and print it to the console
//             // so that the user can learn more about the error and how to fix it.
//             // TODO change error handling
//             System.out.println("Caught an AmazonServiceException, which means your request made it "
//                     + "to Amazon S3, but was rejected with an error response for some reason.");
//             System.out.println("Error Message:    " + ase.getMessage());
//             System.out.println("HTTP Status Code: " + ase.getStatusCode());
//             System.out.println("AWS Error Code:   " + ase.getErrorCode());
//             System.out.println("Error Type:       " + ase.getErrorType());
//             System.out.println("Request ID:       " + ase.getRequestId());
//         }

//         private void handleAmazonClientException(AmazonClientException ace) {
//             System.out.println("Caught an AmazonClientException, which means the client encountered "
//                     + "a serious internal problem while trying to communicate with S3, "
//                     + "such as not being able to access the network.");
//             System.out.println("Error Message: " + ace.getMessage());
//         }


//     }

//     /**
//      * Main method for this Map-Reduce step. Extracts all dependency paths between pairs of nouns that are to be treated
//      * as features in a features vector, in preperation for post-processing in WEKA.
//      * @param args an array of 3 Strings: input path, output path, DPmin.
//      * @throws Exception
//      */
//     public static void main(String[] args) throws Exception {
//         if (args.length != 4)
//             throw new IOException("Phase 1: supply 4 arguments");
//         System.out.println("DPmin is set to: " + Integer.parseInt(args[2]));
//         Configuration conf = new Configuration();
//         conf.set("LOCAL_OR_EMR", String.valueOf(args[3].equals("local")));
//         conf.set("DPMIN", args[2]);
//         Job job = Job.getInstance(conf, "Phase 1");
//         job.setJarByClass(job1.class);
//         job.setMapperClass(Mapper1.class);
//         job.setReducerClass(Reducer1.class);
//         job.setMapOutputKeyClass(Text.class);
//         job.setMapOutputValueClass(Text.class);
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(Text.class);
//         job.setNumReduceTasks(1);
//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));
//         System.out.println("Phase 1 - input path: " + args[0] + ", output path: " + args[1]);
//         if (job.waitForCompletion(true))
//             System.out.println("Phase 1: job completed successfully");
//         else
//             System.out.println("Phase 1: job completed unsuccessfully");
//         Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
//         System.out.println("Number of key-value pairs sent to reducers in phase 1: " + counter.getValue());
//     }

// }


//Phase1
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class job1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private Stemmer stemmer;
        private final String REGEX = "[^a-zA-Z ]+";

        /**
         * Setup the Mapper node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new Stemmer();
        }

        /**
         * Map a key-value pair.
         * @param key the line index of the current line of the input file.
         * @param value the line contents.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] components = value.toString().split("\t");
            String ngram = components[1];
            String[] parts = ngram.split(" ");
            Node[] nodes = getNodes(parts);
            if (nodes == null)
                return;
            Node root = constructParsedTree(nodes);
            searchDependencyPath(root, "", root, context);
        }

        /**
         * Transforms a biarc into a an array of Nodes
         * @param parts an array of Strings, each index being a biarc
         * @return an array of Nodes, each one containing a biarc in an accessible container.
         */
        private Node[] getNodes(String[] parts) {
            Node[] partsAsNodes = new Node[parts.length];
            for (int i = 0; i < parts.length; i++) {
                String[] ngramEntryComponents = parts[i].split("/");
                if (ngramEntryComponents.length != 4)
                    return null;
                ngramEntryComponents[0] = ngramEntryComponents[0].replaceAll(REGEX, "");
                if (ngramEntryComponents[0].replaceAll(REGEX, "").equals(""))
                    return null;
                ngramEntryComponents[1] = ngramEntryComponents[1].replaceAll(REGEX, "");
                if (ngramEntryComponents[1].replaceAll(REGEX, "").equals(""))
                    return null;
                partsAsNodes[i] = new Node(ngramEntryComponents, stemmer);
            }
            return partsAsNodes;
        }

        /**
         * Transforms an array of Nodes into a tree, which represents the dependencies defined in the original biarc.
         * @param nodes an array of Nodes.
         * @return the root of the tree.
         */
        private Node constructParsedTree(Node[] nodes) {
            int rootIndex = 0;
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].getFather() > 0)
                    nodes[nodes[i].getFather() - 1].addChild(nodes[i]);
                else
                    rootIndex = i;
            }
            return nodes[rootIndex];
        }

        /**
         * A recursive method to find all shortest paths between nouns in a syntactic tree.
         * @param node a node that is inquired as to being a start or end of a shortest path.
         * @param acc an accumulator that holds the shortest path so far as a String.
         * @param pathStart the first node of the noun pair.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        private void searchDependencyPath(Node node, String acc, Node pathStart, Context context) throws IOException, InterruptedException {
            if (node.isNoun() && acc.isEmpty())
                for (Node child : node.getChildren())
                    searchDependencyPath(child, node.getDependencyPathComponent(), node, context);
            else if (node.isNoun()) {
                    Text a = new Text(acc + ":" + node.getDependencyPathComponent());
                    Text b = new Text(pathStart.getStemmedWord() + "$" + node.getStemmedWord());
                    context.write(a, b);
                    searchDependencyPath(node, "", node, context);
            } else { // node isn't noun, but the accumulator isn't empty
                for (Node child : node.getChildren())
                    searchDependencyPath(child, acc.isEmpty() ? acc : acc + ":" + node.getDependencyPathComponent(), pathStart, context);
            }
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        private File pathsFile;
        private BufferedWriter bw;
        private long numOfFeatures;
        private final String BUCKET_NAME = "yo-ass3-output-bucket";
        private boolean local;
        private int DPmin;

        /**
         * Setup up a Reducer node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            java.nio.file.Path path = Paths.get("ass3Files");
            if (!Files.exists(path))
                Files.createDirectory(path);
            pathsFile = new File("ass3Files/paths.txt");
            bw = new BufferedWriter(new FileWriter(pathsFile));
            numOfFeatures = 0;
            // local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            local = false;
            DPmin = Integer.parseInt(context.getConfiguration().get("DPMIN"));
            System.out.println("Reducer: DPmin is set to " + DPmin);
        }

        /**
         * Counts how many noun pairs exist for a particular dependency path. If there are more than DPmin, this
         * dependency path would be treated as a feature, thus taking an index in the features vector for each noun pair
         * in the corpus.
         * Each dependency path that is counted as a feature would be written to a local file.
         * @param key a dependency path.
         * @param nounPairs all pairs of nouns which appear in this dependency path in the corpus.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> nounPairs, Context context) throws IOException, InterruptedException {
            HashSet<Text> set = new HashSet<>(DPmin);
            for (Text nounPair : nounPairs) {
                if (set.size() == DPmin)
                    break;
                else if (set.contains(nounPair))
                    continue;
                else
                    set.add(nounPair);
            }
            if (set.size() >= DPmin) {
                bw.write(key.toString() + "\n");
                numOfFeatures++;
                for (Text nounPair : nounPairs)
                    context.write(nounPair, key);
            }
        }

        /**
         * Writes 2 files to an S3 bucket:
         * 1. the local file, containing all dependency paths that are counted as features
         * 2. a file which contains the number of features, i.e. the number of dependency paths with more than DPmin noun pairs
         * @throws IOException
         */
        @Override
        public void cleanup(Context context) throws IOException {
            System.out.println("Features vector length: " + numOfFeatures);
            bw.close();
            File numOfFeaturesFile = new File("ass3Files/numOfFeatures.txt");
            bw = new BufferedWriter(new FileWriter(numOfFeaturesFile));
            bw.write(numOfFeatures + "\n");
            bw.close();
            if (local) {
                Files.copy(new File("ass3Files/paths.txt").toPath(), new File("ass3Files/pathsListCopy.txt").toPath(), REPLACE_EXISTING);
            } else {
                AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                try {
                    System.out.print("Uploading the dependency paths file to S3... ");
                    s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/paths.txt", pathsFile));
                    System.out.println("Done.");
                    System.out.print("Uploading num of features file to S3... ");
                    s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/numOfFeatures.txt", numOfFeaturesFile));
                    System.out.println("Done.");
                } catch (Exception ase) {
                    System.out.println("Caught an AmazonServiceException, which means your request made it "
                            + "to Amazon S3, but was rejected with an error response for some reason.");
                    // System.out.println("Error Message:    " + ase.getMessage());
                    // System.out.println("HTTP Status Code: " + ase.getStatusCode());
                    // System.out.println("AWS Error Code:   " + ase.getErrorCode());
                    // System.out.println("Error Type:       " + ase.getErrorType());
                    // System.out.println("Request ID:       " + ase.getRequestId());
                }
                // } catch (Exception ace) {
                //     System.out.println("Caught an AmazonClientException, which means the client encountered "
                //             + "a serious internal problem while trying to communicate with S3, "
                //             + "such as not being able to access the network.");
                //     // System.out.println("Error Message: " + ace.getMessage());
                // }
            }
        }

    }

    /**
     * Main method for this Map-Reduce step. Extracts all dependency paths between pairs of nouns that are to be treated
     * as features in a features vector, in preperation for post-processing in WEKA.
     * @param args an array of 3 Strings: input path, output path, DPmin.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IOException("Phase 1: supply 4 arguments");
        System.out.println("DPmin is set to: " + Integer.parseInt(args[2]));
        Configuration conf = new Configuration();
        conf.set("LOCAL_OR_EMR", String.valueOf(args[3].equals("local")));
        conf.set("DPMIN", args[2]);
        Job job = Job.getInstance(conf, "Phase 1");
        job.setJarByClass(job1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 1 - input path: " + args[0] + ", output path: " + args[1]);
        if (job.waitForCompletion(true))
            System.out.println("Phase 1: job completed successfully");
        else
            System.out.println("Phase 1: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Number of key-value pairs sent to reducers in phase 1: " + counter.getValue());
    }

}