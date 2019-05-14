package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Tuesday 4 September 1632");
		inputData.add("ERROR: Tuesday 4 September 1854");
		inputData.add("WARN: Tuesday 4 September 1942");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
				
		JavaPairRDD<String, String> pairRdd = originalLogMessages.mapToPair(rawValue -> {
			String[] columns = rawValue.split(":");
			String level = columns[0];
			String date = columns[1];
			
			return new Tuple2<String, String>(level, date);
			
		});
		
		sc.close();

	}

}
