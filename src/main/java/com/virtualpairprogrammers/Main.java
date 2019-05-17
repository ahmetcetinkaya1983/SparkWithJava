package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);
		
		//Warmup
		//JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
		//.reduceByKey((value1, value2) -> value1+value2);
		//chapterCountRdd.collect().forEach(System.out::println);
		
		//Step-1 Removing duplicate Views
		JavaPairRDD<Integer, Integer> distinctViews = viewData.distinct();
		distinctViews.collect().forEach(System.out::println);
		
		
		sc.close();

	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode) {
			
			//(chapterId, Title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder sarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewving_figures/titles.csv")
						.mapToPair(commaSeperatedLine -> {
							String[] cols = commaSeperatedLine.split(",");
							return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
						});
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode) {
			//(chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,1));
			rawChapterData.add(new Tuple2<>(97,1));
			rawChapterData.add(new Tuple2<>(98,1));
			rawChapterData.add(new Tuple2<>(99,2));
			rawChapterData.add(new Tuple2<>(100,3));
			rawChapterData.add(new Tuple2<>(101,3));
			rawChapterData.add(new Tuple2<>(102,3));
			rawChapterData.add(new Tuple2<>(103,3));
			rawChapterData.add(new Tuple2<>(104,3));
			rawChapterData.add(new Tuple2<>(105,3));
			rawChapterData.add(new Tuple2<>(106,3));
			rawChapterData.add(new Tuple2<>(107,3));
			rawChapterData.add(new Tuple2<>(108,3));
			rawChapterData.add(new Tuple2<>(109,3));
			return sc.parallelizePairs(rawChapterData);
			
		}
		return sc.textFile("src/main/resources/viewving_figures/chapters.csv")
				.mapToPair(commaSeperatedLine -> {
					String[] cols = commaSeperatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
				});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		if(testMode) {
			//chapterViews - (userId, chapterId))
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14,96));
			rawViewData.add(new Tuple2<>(14,97));
			rawViewData.add(new Tuple2<>(13,96));
			rawViewData.add(new Tuple2<>(13,96));
			rawViewData.add(new Tuple2<>(13,96));
			rawViewData.add(new Tuple2<>(14,99));
			rawViewData.add(new Tuple2<>(13,100));
			return sc.parallelizePairs(rawViewData);
		}
		return sc.textFile("src/main/resources/viewving_figures/views-*.csv")
				.mapToPair(commaSeperatedLine -> {
					String[] cols = commaSeperatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
				});
	}

}
