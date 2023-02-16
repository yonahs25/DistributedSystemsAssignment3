package com.mycompany.app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** This file is the main driver class for Job 1 
and is responsible for setting up and running the job. 
It extends Configured and implements Tool so that it can be run using the ToolRunner. 
The run method sets up the job by specifying the Mapper and Reducer classes, 
the input and output format classes, the output key and value classes, 
the input and output paths, and then submits the job using waitForCompletion. 
The main method simply runs the Job1Driver class using the ToolRunner and exits with the exit code returned from the run method.
*/
public class job1Driver extends Configured implements Tool {
   public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf(), "Job1");
      job.setJarByClass(job1Driver.class);
      job.setMapperClass(job1Mapper.class);
      job.setReducerClass(job1Reducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new job1Driver(), args);
      System.exit(exitCode);
   }
}
