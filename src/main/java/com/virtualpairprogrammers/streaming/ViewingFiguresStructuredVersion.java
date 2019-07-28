package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Durations;

public class ViewingFiguresStructuredVersion {

	public static void main(String[] args) throws StreamingQueryException {
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
		df.createOrReplaceTempView("viewing_figures");
		
		//key, value, timestamp
		Dataset<Row> results = 
				session.sql("select window, cast(value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp, '2 minutes'),course_name ");
		
		StreamingQuery query = results
				.writeStream()
				.format("console")
				.outputMode(OutputMode.Complete())
				.option("truncate", false)
				.option("numRows", 50)
				.start();
//				.trigger(Trigger.ProcessingTime(Durations.minutes(1))) //used for batch timing
		query.awaitTermination();
		
	}

}
