package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class LogStreamAnalysis {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
		
		JavaStreamingContext sc  = new JavaStreamingContext(conf, Durations.seconds(30));
		
		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);
		
		JavaDStream<Object> results = inputData.map(item -> item);
		
		results.print();
		
		sc.start();
		sc.awaitTermination();
		
	}

}
