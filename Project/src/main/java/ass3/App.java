package ass3;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class App 
{
    public static void main( String[] args )
    {
        String output_bucket = "yo-ass3-output-bucket";
        AWSCredentialsProvider cp = new ProfileCredentialsProvider();
        AWSCredentials credentials = cp.getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        //TODO need to know name of the jar

        HadoopJarStepConfig jarStep1 = new HadoopJarStepConfig()
                    .withJar("s3://oy-jars-bucket/Ass2-1.0-SNAPSHOT.jar")
                    .withMainClass("Phase1")
                    .withArgs("s3n://dsps162assignment3benasaf/input2", "hdfs:///intermediate/", args[0], "emr");

            StepConfig step1Config = new StepConfig()
                    .withName("Phase 1")
                    .withHadoopJarStep(jarStep1)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");

            HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
                    .withJar("s3://oy-jars-bucket/Ass2-1.0-SNAPSHOT.jar")
                    .withMainClass("Phase2")
                    .withArgs("hdfs:///intermediate/", "s3n://" + output_bucket + "/output_single_corpus", "emr");//change output bucket path

            StepConfig step2Config = new StepConfig()
                    .withName("Phase 2")
                    .withHadoopJarStep(jarStep2)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");

            JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                    .withInstanceCount(5)
                    .withMasterInstanceType(InstanceType.M4Large.toString())
                    .withSlaveInstanceType(InstanceType.M4Large.toString())
                    .withHadoopVersion("2.6.0")
                    .withEc2KeyName("something")
                    .withKeepJobFlowAliveWhenNoSteps(false)
                    .withPlacement(new PlacementType("us-east-1a"));

            RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                    .withName("hypernyms")
                    .withInstances(instances)
                    .withSteps(step1Config, step2Config)
                    .withServiceRole("EMR_DefaultRole")
                    .withJobFlowRole("EMR_EC2_DefaultRole")
                    .withLogUri("s3n://" + output_bucket + "/logs/")
                    .withReleaseLabel("emr-4.2.0")
                    .withBootstrapActions();


            RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

    }
}
