package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Main {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

//		moduleOne();

        moduleTwo();


    }

    private static void moduleTwo() {
        SparkSession sparkSession = SparkSession
				.builder()
				.appName("sparkSql")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();

		Dataset<Row> rowDataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
//		rowDataset.show();

//        long count = rowDataset.count();
//        System.out.println("There are "+ count + " recordss");

        Row firstRow = rowDataset.first();
        String subject = firstRow.getAs("subject");
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(year);

//        Dataset<Row> modernArtResults = rowDataset.filter("subject = 'Modern Art' AND year >= 2007");
//        Dataset<Row> modernArtResults = rowDataset.filter(row -> row.getAs("subject").equals("ModernArt") && Integer.parseInt(row.getAs("year")) >= 2007);
//        Column subjectColumn = rowDataset.col("subject");
//        Column yearColumn = rowDataset.col("year");
//        Dataset<Row> modernArtResults = rowDataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));

        rowDataset.createOrReplaceTempView("students" );
//        Dataset<Row> modernArtResults = sparkSession.sql("select score, year from students where subject = 'Modern Art' AND year >= 2007");
        Dataset<Row> modernArtResults = sparkSession.sql("select distinct year from students where subject = 'Modern Art' AND year >= 2007");

        modernArtResults.show();

        sparkSession.close();
	}

    private static void moduleOne() {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long, String>> results = sorted.take(10);

        results.forEach(System.out::println);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
    }
}
