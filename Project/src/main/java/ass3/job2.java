package ass3;
// aws imports
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
// hadoop imports
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
// java imports
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class job2 {
    private static final String BUCKET = "dsps162assignment3benasaf";
    private static final String PATHS_LIST = "resource/paths.txt";
    private static final String NUM_OF_FEATURES_FILE = "resource/numOfFeatures.txt";
    private static final String HYPERNYM_LIST = "resource/hypernym.txt";

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, LongPairWritable> {
        private LongPairWritable count;
        private File pathsListCopy;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Read the paths list from S3.
            // Create a copy of the paths list in the local file system.
            // This is done in order to avoid reading the paths list from S3 for every mapper.
            // The copy of the paths list is used by the findIndexInPathsList method.
            // The copy of the paths list is deleted in the cleanup method.
            count = new LongPairWritable(0, 1);
            boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            BufferedReader br;
            if (local) {
                br = new BufferedReader(new FileReader(PATHS_LIST));
            } else {
                // Create an instance of Amazon S3 client
                AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                // Read the object from S3 and convert its contents to a stream of bytes
                S3Object object = s3.getObject(new GetObjectRequest(BUCKET, PATHS_LIST));
                InputStream inputStream = object.getObjectContent();
                br = new BufferedReader(new InputStreamReader(inputStream));
            }
            java.nio.file.Path path = Paths.get("resource");
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
            pathsListCopy = new File("resource/pathsListCopy.txt");
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
            // The mapper receives a line from the output of the previous phase.
            // The line is in the following format: <word> <path> <count>
            // The mapper extracts the word and the path from the line.
            // The mapper finds the index of the path in the paths list.
            // The mapper emits the word and a pair of the index and the count.
            String[] parts = value.toString().split("\\s");
            int index = utils.findIndexInPathsList(parts[1], pathsListCopy);
            if (index != -1) {
                count.setFirst(index);
                context.write(new Text(parts[0]), count);
            }
        }

        
    }

    public static class Reducer2 extends Reducer<Text, LongPairWritable, Text, Text> {

    private HashMap<String, Boolean> testSet;
    private final String BUCKET = "";
    private final String HYPERNYM_LIST = "resource/hypernym.txt";
    private final String NUM_OF_FEATURES_FILE = "resource/numOfFeatures.txt";
    private long numOfFeatures;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Read the dictionary file from S3.
        // Create a copy of the dictionary file in the local file system.
        // This is done in order to avoid reading the dictionary file from S3 for every reducer.
        // The copy of the dictionary file is used by the loadDictionary method.
        // The copy of the dictionary file is deleted in the cleanup method.
        
        // Get the bucket and dictionary file name from the configuration
        String bucketName = context.getConfiguration().get("BUCKET_NAME");
        String dictionaryFileName = context.getConfiguration().get("DICTIONARY_FILE_NAME");

        // Create an S3 client and get the dictionary file object
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, dictionaryFileName));

        // Read the dictionary file into a BufferedReader
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

        // Create a local copy of the dictionary file
        Path dictionaryCopyPath = Paths.get("dictionary_copy.txt");
        if (!Files.exists(dictionaryCopyPath)) {
            Files.createFile(dictionaryCopyPath);
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(dictionaryCopyPath.toFile()));

        // Copy the contents of the dictionary file to the local copy
        String line;
        while ((line = br.readLine()) != null) {
            bw.write(line + "\n");
        }

        // Close the BufferedReader and BufferedWriter
        br.close();
        bw.close();
    }


    @Override
    public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
        HashMap<Long, Long> features = new HashMap<>();
        long count = 0;
        
        for (LongPairWritable value : values) {
            long index = value.getFirst();
            long val = value.getSecond();
            utils.updateFeaturesMap(features, index, val);
            count += val;
        }
    
        if (count > 10) { // filter
            StringBuilder vector = utils.createVectorFromFeatures(features, numOfFeatures, key.toString(), testSet);
            writeVectorToContext(context, key, vector.toString());
        }
    }
    
    /**
     * Writes the given vector string to the context, using the given key to determine whether
     * the output should be written to the training or test set.
     */
    private void writeVectorToContext(Context context, Text key, String vector) throws IOException, InterruptedException {
        if (vector.charAt(0) == 't') {
            context.write(new Text("test"), new Text(vector.substring(5)));
        } else {
            context.write(new Text("train"), new Text(vector.substring(6)));
        }
    }    
}
}