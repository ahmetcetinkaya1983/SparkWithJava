package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("startingSpark").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
												   .getOrCreate();
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> results =spark.sql("select level, date_format(datetime, 'MMM') as month from logging_table");
		
		results.createOrReplaceTempView("logging_table");
		results = spark.sql("select level, month, count(1)as total from logging_table group by level, month");
		results.show(200);
		
		results.createOrReplaceTempView("results_table");
		Dataset<Row> totals = spark.sql("select sum(total) from results_table");
		totals.show();
	}
}