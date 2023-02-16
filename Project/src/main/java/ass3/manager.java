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

public class manager {

    private static String input_bucket;
    private static String output_bucket;

    private static String set_input_output_path_for_job (Job job , String input_path)  throws IOException{
        FileInputFormat.addInputPath(job, new Path(input_path));
        String path = output_bucket + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static String phase1_setter(Job job, String input_path) 
    {

        return "";
    }

    private static String phase2_setter(Job job, String input_path)
    {

        return "";
    }

    
    public static void main (String[] args)  throws IOException ,   ClassNotFoundException, InterruptedException 
    {
        input_bucket = args[0];
        output_bucket = args[1];
        output_bucket = output_bucket.concat("/" + System.currentTimeMillis()+"/");
        String DPmin = args[2];


        Configuration phase1_conf = new Configuration();
        phase1_conf.set("DPMIN", DPmin);
        final Job phase1_job = Job.getInstance(phase1_conf, "phase1");
        String phase1_path = phase1_setter(phase1_job, input_bucket);
        if (phase1_job.waitForCompletion(true)) {
            System.out.println("phase 1 finished");
        } else {
            System.out.println("phase1 failed");
            System.exit(1);
        }

        Configuration phase2_conf = new Configuration();
        phase1_conf.set("DPMIN", DPmin);
        final Job phase2_job = Job.getInstance(phase2_conf, "phase1");
        String phase2_path = phase1_setter(phase2_job, input_bucket);
        if (phase1_job.waitForCompletion(true)) {
            System.out.println("phase 2 finished");
        } else {
            System.out.println("phase 2 failed");
            System.exit(1);
        }

    }
}
