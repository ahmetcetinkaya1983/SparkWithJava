package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {

		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
				
		JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
		
		sc.close();

	}

}
