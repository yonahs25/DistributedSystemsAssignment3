package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

   @Override
   protected void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
      // Parse the input value (a line of text from the corpus)
      String[] nounPairs = value.toString().split("\t");
      String noun1 = nounPairs[0];
      String noun2 = nounPairs[1];
      
      // Emit the noun pair as the key and the word "1" as the value
      context.write(new Text(noun1 + "," + noun2), new Text("1"));
   }
}
