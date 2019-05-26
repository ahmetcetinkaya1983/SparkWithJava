package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("startingSpark").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
												   .getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		
		long numberOfRows = dataset.count();
		System.out.println("there are "+numberOfRows+" records");
		
		Row firstRow = dataset.first();
		String subject = firstRow.get(2).toString();// get subject of firstrow
		//String subject = firstRow.getAs("subject").toString();// we can use getAs(columnName) too
		System.out.println(subject);
		
		int year =Integer.parseInt(firstRow.getAs("year"));
		System.out.println("the year was "+year);

	}
}