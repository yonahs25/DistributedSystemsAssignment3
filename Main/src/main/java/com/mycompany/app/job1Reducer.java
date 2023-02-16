package com.mycompany.app;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

   @Override
   protected void reduce(Text key, Iterable<IntWritable> values, Context context)
         throws IOException, InterruptedException {
      // Sum the values for each key (noun pair)
      int sum = 0;
      for (IntWritable value : values) {
         sum += value.get();
      }
      
      // Emit the noun pair as the key and the sum as the value
      context.write(key, new IntWritable(sum));
   }
}
