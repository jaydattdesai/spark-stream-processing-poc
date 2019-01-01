package com.jaydatt.spartkpoc;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import scala.Tuple2;

//@SpringBootApplication
public class SparkStreamProcessingPocApplication {

	public static void main(String[] args) throws InterruptedException {

//		SpringApplication.run(SparkStreamProcessingPocApplication.class, args);
//		
//		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
//		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//		 JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
//		            "localhost", 8080, StorageLevels.MEMORY_AND_DISK_SER);
//
//		// Split each line into words
//		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//
//		// Count each word in each batch
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
//		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
//
//		// Print the first ten elements of each RDD generated in this DStream to the
//		// console
//
//		wordCounts.print();
//		
//		jssc.start(); // Start the computation
//		jssc.awaitTermination();
		
		
		SpringApplication.run(SparkStreamProcessingPocApplication.class, args);
		
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

		JavaDStream<String> lines = jssc.textFileStream("/home/jaydatt/Documents/workspace/spark-stream-processing-poc/filetoread");
		
		// Split each line into words. 
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).filter(f -> f.startsWith("name"));

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the
		// console

		wordCounts.print();
		
		jssc.start(); // Start the computation
		jssc.awaitTermination();
	}

}
