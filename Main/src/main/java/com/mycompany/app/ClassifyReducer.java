package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
The ClassifyReducer class is responsible for aggregating the results 
of the classification performed by the ClassifyMapper class. 
In this class, you'll need to implement the reduce() method, 
which takes as input a key and a set of values, 
and performs the necessary computations to aggregate the values into a single output.
 */

public class ClassifyReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    // Initialize counters for true positives, false positives, and false negatives
    int truePositives = 0;
    int falsePositives = 0;
    int falseNegatives = 0;

    // Iterate through the values and update the counters based on the classifier's output
    for (Text value : values) {
      String[] parts = value.toString().split("\t");
      String actualClass = parts[0];
      String predictedClass = parts[1];
      if (actualClass.equals("True") && predictedClass.equals("True")) {
        truePositives++;
      } else if (actualClass.equals("False") && predictedClass.equals("True")) {
        falsePositives++;
      } else if (actualClass.equals("True") && predictedClass.equals("False")) {
        falseNegatives++;
      }
    }

    // Compute precision, recall, and F1 measure
    float precision = (float) truePositives / (truePositives + falsePositives);
    float recall = (float) truePositives / (truePositives + falseNegatives);
    float f1 = 2 * precision * recall / (precision + recall);

    // Write the results to the output
    context.write(key, new Text(String.format("Precision: %.2f, Recall: %.2f, F1: %.2f", precision, recall, f1)));
  }
}
