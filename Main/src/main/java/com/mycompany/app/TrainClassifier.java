package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;

/**
This file would be responsible for training the classifier using WEKA
 */
public class TrainClassifier {

  public static void main(String[] args) throws Exception {

  // Read the preprocessed data in the arff file
  BufferedReader reader = new BufferedReader(new FileReader(args[0]));
  Instances instances = new Instances(reader);
  reader.close();

  // Set the class index to the last attribute
  instances.setClassIndex(instances.numAttributes() - 1);

  // Train the classifier using NaiveBayes
  NaiveBayes classifier = new NaiveBayes();
  classifier.buildClassifier(instances);

  // Evaluate the classifier using 10-fold cross-validation
  Evaluation evaluation = new Evaluation(instances);
  evaluation.crossValidateModel(classifier, instances, 10, new Random(1));

  // Print the precision, recall, and F1 measures
  System.out.println(evaluation.toClassDetailsString());
  System.out.println(evaluation.toMatrixString());
  System.out.println(evaluation.toSummaryString());
}

}


