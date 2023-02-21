package ass3;

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

public class job1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private Stemmer stemmer;
        private final String REGEX = "[^a-zA-Z ]+";

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new Stemmer();
        }

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

        private void searchDependencyPath(Node node, String acc, Node pathStart, Context context) throws IOException, InterruptedException {
            if (node.isNoun() && acc.isEmpty())
                for (Node child : node.getChildren())
                    searchDependencyPath(child, node.getDependencyPathComponent(), node, context);
            else if (node.isNoun()) {
                    Text a = new Text(acc + ":" + node.getDependencyPathComponent());
                    Text b = new Text(pathStart.getStemmedWord() + "$" + node.getStemmedWord());
                    context.write(a, b);
                    searchDependencyPath(node, "", node, context);
            } else { 
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
                }
            }
        }
    }


    /////////////////REFACTORED CODE////////////////////
public static class DependencyPathMapper extends Mapper<LongWritable, Text, Text, Text> {
    // Initialize a Stemmer object to use later on in the mapper
    private Stemmer stemmer;
    // Regular expression pattern to remove non-alphabetic and non-space characters
    private final String REGEX = "[^a-zA-Z ]+";

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // Initialize the Stemmer object in the setup method
        stemmer = new Stemmer();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input text by the tab character
        String[] components = value.toString().split("\t");
        String ngram = components[1];
        // Split the ngram by space to get its component parts
        String[] ngramParts = ngram.split(" ");
        // Convert the ngram parts into an array of Nodes
        Node[] nodes = convertToNodes(ngramParts);
        if (nodes == null)
            return;
        // Construct the parsed tree from the array of nodes
        Node root = constructParsedTree(nodes);
        // Search for dependency paths in the parsed tree and write the results to the output context
        searchDependencyPath(root, "", root, context);
    }

    private Node[] convertToNodes(String[] ngramParts) {
        Node[] nodes = new Node[ngramParts.length];
        for (int i = 0; i < ngramParts.length; i++) {
            // Split each ngram part by "/" to get its component parts
            String[] ngramEntryComponents = ngramParts[i].split("/");
            // Check that the ngram entry has 4 components
            if (ngramEntryComponents.length != 4)
                return null;
            // Remove non-alphabetic and non-space characters from the first and second components of the ngram entry
            ngramEntryComponents[0] = ngramEntryComponents[0].replaceAll(REGEX, "");
            ngramEntryComponents[1] = ngramEntryComponents[1].replaceAll(REGEX, "");
            // Check that the first and second components of the ngram entry are not empty
            if (ngramEntryComponents[0].isEmpty() || ngramEntryComponents[1].isEmpty())
                return null;
            // Create a new Node object from the ngram entry components and add it to the array of nodes
            nodes[i] = new Node(ngramEntryComponents, stemmer);
        }
        return nodes;
    }

    private Node constructParsedTree(Node[] nodes) {
        // Find the root of the parsed tree by looking for the node with no parent
        int rootIndex = 0;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getFather() > 0)
                nodes[nodes[i].getFather() - 1].addChild(nodes[i]);
            else
                rootIndex = i;
        }
        return nodes[rootIndex];
    }
    
    private void searchDependencyPath(Node node, String acc, Node pathStart, Context context) throws IOException, InterruptedException {
        if (node.isNoun() && acc.isEmpty()) {
            // If this node is a noun and the accumulator is empty, then its children may be a possible start of a dependency path.
            for (Node child : node.getChildren()) {
                searchDependencyPath(child, node.getDependencyPathComponent(), node, context);
            }
        } else if (node.isNoun()) {
            // If this node is a noun and the accumulator is non-empty, then we have a dependency path.
            Text dependencyPath = new Text(acc + ":" + node.getDependencyPathComponent());
            Text dependencyPathData = new Text(pathStart.getStemmedWord() + "$" + node.getStemmedWord());
            context.write(dependencyPath, dependencyPathData);
            searchDependencyPath(node, "", node, context);
        } else {
            // If this node is not a noun, search for dependency paths recursively on its children.
            for (Node child : node.getChildren()) {
                String newAcc = acc.isEmpty() ? acc : acc + ":" + node.getDependencyPathComponent();
                searchDependencyPath(child, newAcc, pathStart, context);
            }
        }
    }
}

public static class MyReducer extends Reducer<Text, Text, Text, Text> {
    
    private File pathsFile;
    private BufferedWriter bw;
    private long numOfFeatures;
    private final String BUCKET_NAME = "yo-ass3-output-bucket";
    private boolean local;
    private int DPmin;
    
    /**
     * Set up the Reducer before processing input data.
     * 
     * @param context  The Context object for this job.
     * @throws IOException If an I/O error occurs while creating the output files.
     */
    public void setup(Context context) throws IOException {
        // Create a directory to store the output files
        java.nio.file.Path path = Paths.get("ass3Files");
        if (!Files.exists(path))
            Files.createDirectory(path);
        
        // Create a new file for storing the dependency paths
        pathsFile = new File("ass3Files/paths.txt");
        bw = new BufferedWriter(new FileWriter(pathsFile));
        
        // Initialize variables for feature count and DPmin value
        numOfFeatures = 0;
        DPmin = Integer.parseInt(context.getConfiguration().get("DPMIN"));
        
        // Determine whether job is being run locally or on an EMR cluster
        local = false;  // context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
        
        // Log the value of DPmin
        System.out.println("Reducer: DPmin is set to " + DPmin);
    }

    /**
     * Process each input key-value pair to determine the set of unique noun pairs 
     * associated with the current key. If the set has size >= DPmin, write the key 
     * to the paths file and emit each noun pair with the key.
     * 
     * @param key         The input key to process.
     * @param nounPairs   An Iterable of Text objects representing the associated noun pairs.
     * @param context     The Context object for this job.
     * @throws IOException If an I/O error occurs while writing to the output files.
     * @throws InterruptedException If a thread is interrupted during the reduce operation.
     */
    @Override
    public void reduce(Text key, Iterable<Text> nounPairs, Context context) throws IOException, InterruptedException {
        // Initialize a HashSet to store unique noun pairs associated with the key
        HashSet<Text> set = new HashSet<>(DPmin);
        
        // Iterate over the nounPairs Iterable to fill the HashSet
        for (Text nounPair : nounPairs) {
            if (set.size() == DPmin) {
                break;
            } else if (set.contains(nounPair)) {
                continue;
            } else {
                set.add(nounPair);
            }
        }
        
        // If the HashSet has size >= DPmin, write the key to the paths file and emit each noun pair with the key
        if (set.size() >= DPmin) {
            bw.write(key.toString() + "\n");
            numOfFeatures++;
            for (Text nounPair : nounPairs) {
                context.write(nounPair, key);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException {
        // print the number of features
        System.out.println("Features vector length: " + numOfFeatures);
        // close the file writer
        bw.close();
        // write the number of features to a file
        boolean local = true;
        File numOfFeaturesFile = new File("ass3Files/numOfFeatures.txt");
        bw = new BufferedWriter(new FileWriter(numOfFeaturesFile));
        bw.write(numOfFeatures + "\n");
        bw.close();
        // copy the paths file to a new location if running locally
        if (local) {
            Files.copy(new File("ass3Files/paths.txt").toPath(), new File("ass3Files/pathsListCopy.txt").toPath(), REPLACE_EXISTING);
        } 
        // upload the paths file and number of features file to S3 if running on EMR
        else {
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            try {
                // upload the paths file to S3
                System.out.print("Uploading the dependency paths file to S3... ");
                s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/paths.txt", pathsFile));
                System.out.println("Done.");
                // upload the number of features file to S3
                System.out.print("Uploading num of features file to S3... ");
                s3.putObject(new PutObjectRequest(BUCKET_NAME, "ass3Files/numOfFeatures.txt", numOfFeaturesFile));
                System.out.println("Done.");
            } catch (Exception ase) {
                System.out.println("Caught an AmazonServiceException, which means your request made it "
                        + "to Amazon S3, but was rejected with an error response for some reason."); 
            }
        }
    }
}
}

    