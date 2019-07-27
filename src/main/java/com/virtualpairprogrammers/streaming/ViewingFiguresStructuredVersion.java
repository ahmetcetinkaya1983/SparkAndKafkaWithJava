package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ViewingFiguresStructuredVersion {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("StructuredViewingReport")
				.getOrCreate();
		
		Dataset<Row> df = session.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe","viewrecords")
				.load();
		
		//start some dataframe operations
		
	}

}
