package ass3;
import java.io.IOException;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.LocalTime;  
import com.amazonaws.services.s3.model.S3ObjectInputStream;


public class manager {

    private static String input_bucket;
    private static String output_bucket;

    private static String set_input_output_path_for_job (Job job , String input_path)  throws IOException{
        FileInputFormat.addInputPath(job, new Path(input_path));
        String path = output_bucket + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static String job1_setter(Job job, String input_path) throws IOException ,   ClassNotFoundException, InterruptedException
    {
        job.setJarByClass(job1.class);
        job.setMapperClass(job1.Mapper1.class);
        job.setReducerClass(job1.Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return set_input_output_path_for_job(job, input_path);
    }

    private static String job2_setter(Job job, String input_path, String output_bucket) throws IOException ,   ClassNotFoundException, InterruptedException
    {
        job.setJarByClass(job2.class);
        job.setMapperClass(job2.Mapper2.class);
        job.setReducerClass(job2.Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(input_path));
        String path = output_bucket + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;
    }

    
    public static void main (String[] args)  throws IOException ,   ClassNotFoundException, InterruptedException 
    {
        input_bucket = args[0];
        output_bucket = args[1];
        output_bucket = output_bucket.concat("/" + System.currentTimeMillis()+"/");
        String DPmin = args[2];


        Configuration job1_conf = new Configuration();
        job1_conf.set("DPMIN", DPmin);
        final Job job1_job = Job.getInstance(job1_conf, "job1");
        String job1_path = job1_setter(job1_job, input_bucket);
        if (job1_job.waitForCompletion(true)) {
            System.out.println("job 1 finished");
        } else {
            System.out.println("job1 failed");
            System.exit(1);
        }

        Configuration job2_conf = new Configuration();
        final Job job2_job = Job.getInstance(job2_conf, "job2");
        String job2_path = job2_setter(job2_job, job1_path, output_bucket);
        if (job2_job.waitForCompletion(true)) {
            System.out.println("job 2 finished");
        } else {
            System.out.println("job 2 failed");
            System.exit(1);
        }

    }
}
