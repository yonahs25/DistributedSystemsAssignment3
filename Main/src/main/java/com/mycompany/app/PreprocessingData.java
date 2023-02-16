package com.mycompany.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringReader;
import java.util.ArrayList;
import weka.core.Instances;

/**
 * PreprocessingData
 *  This file would be responsible for reading the txt file you 
    obtained from the first MapReduce job and preprocessing the 
    data so that it is in the correct format for WEKA.
 */
public class PreprocessingData{
    public static void main(String[] args) throws Exception {
  
         // Read the txt file and store it in a StringBuilder
        StringBuilder data = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String line;
        while ((line = br.readLine()) != null) {
            data.append(line).append("\n");
        }
        br.close();

        // Convert the StringBuilder to an Instances object
        Instances instances = new Instances(new BufferedReader(new StringReader(data.toString())));

        // Set the class index to the last attribute
        instances.setClassIndex(instances.numAttributes() - 1);

        // Store the preprocessed data in a new arff file
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
        bw.write(instances.toString());
        bw.close();
    }
}