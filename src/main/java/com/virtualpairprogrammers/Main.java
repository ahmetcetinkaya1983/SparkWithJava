package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
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
import static org.apache.spark.sql.functions.*;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("startingSpark").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
												   .getOrCreate();
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		//dataset.createOrReplaceTempView("logging_table");
		
		/* sql grouping 
		Dataset<Row> results = spark.sql(
				"select level, date_format(datetime, 'MMMM') as month, count(1) as total "+
				"from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level"
						);
						*/
		
		/* or instead, dataframe grouping
		dataset = dataset.select(col("level"), 	date_format(col("datetime"),"MMMM").alias("month"), 
												date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
		dataset = dataset.orderBy(col("monthnum"), col("level"));
		dataset = dataset.drop(col("monthnum"));
		*/
		
		//or using pivot tables which are more user-friendly
		dataset = dataset.select(col("level"), 	date_format(col("datetime"),"MMMM").alias("month"), 
				date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		Object[] months = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August", "Augcember", "September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(months);
 		
		
		dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);;
		
		dataset.show(100);

	}
}