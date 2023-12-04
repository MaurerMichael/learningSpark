package com.virtualpairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;


import javax.swing.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSparkLearning")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

//        integerExample(sparkContext);

//        stringExample(sparkContext);

//        flatMapExample(sparkContext);

//        readingFileIntoSpark(sparkContext);

//        joinExample(sparkContext);

//        leftOuterJoinExample(sparkContext);

//        rightOuterJoinExample(sparkContext);

//        fullOuterJoinExample(sparkContext);

//        cartesiansExample(sparkContext);

        abschnitt13Warmup(sparkContext);

//        abschnitt13(sparkContext);

        sparkContext.close();
    }

    public static void abschnitt13(JavaSparkContext sc) {

    }

    public static void abschnitt13Warmup(JavaSparkContext sc) {

    }

    public static void cartesiansExample(JavaSparkContext sparkContext) {
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Marybelle"));
        userRaw.add(new Tuple2<>(6, "Raquel"));


        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(userRaw);

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> rightOuterJoinedRdd = visits.cartesian(users);

        rightOuterJoinedRdd.foreach(v -> System.out.println("user " + v._2._2 + ", visits " + v._2._1));

    }

    public static void fullOuterJoinExample(JavaSparkContext sparkContext) {
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Marybelle"));
        userRaw.add(new Tuple2<>(6, "Raquel"));


        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(userRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> rightOuterJoinedRdd = visits.fullOuterJoin(users);

        rightOuterJoinedRdd.foreach(v -> System.out.println("user " + v._2._2.orElse("NO_NAME") + ", visits " + v._2._1.orElse(0)));

    }

    public static void rightOuterJoinExample(JavaSparkContext sparkContext) {
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Marybelle"));
        userRaw.add(new Tuple2<>(6, "Raquel"));


        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(userRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinedRdd = visits.rightOuterJoin(users);

        rightOuterJoinedRdd.foreach(v -> System.out.println("user " + v._2._2 + ", visits " + v._2._1.orElse(0)));
    }


    public static void leftOuterJoinExample(JavaSparkContext sparkContext) {
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Marybelle"));
        userRaw.add(new Tuple2<>(6, "Raquel"));


        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(userRaw);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinedRdd = visits.leftOuterJoin(users);

        leftOuterJoinedRdd.foreach(v -> System.out.println(v));

        leftOuterJoinedRdd
                .map(it -> it._2._2)
                .filter(Optional::isPresent)
                .map(v -> v.get().toUpperCase())
                .foreach(v -> System.out.println(v));

    }

    public static void joinExample(JavaSparkContext sparkContext) {
        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Marybelle"));
        userRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(userRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);

        joinedRdd.foreach(v -> System.out.println(v));

    }

    public static void readingFileIntoSpark(JavaSparkContext sparkContext) {
        JavaRDD<String> initialRdd = sparkContext.textFile("src/main/resources/subtitles/input.txt");

        initialRdd
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(word -> !word.trim().isEmpty())
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<String, Long>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(tuple -> System.out.println("V1: " + tuple._1 + ", V2: " + tuple._2));

    }

    public static void flatMapExample(JavaSparkContext sparkContext) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        sparkContext
                .parallelize(inputData)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(word -> !word.matches("\\d*"))
                .foreach(v -> System.out.println(v));
    }

    public static void stringExample(JavaSparkContext sparkContext) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");


        sparkContext
                .parallelize(inputData)
                .mapToPair(rawValue ->
                        new Tuple2<String, Long>(rawValue.split(":")[0], 1L)
                )
                .reduceByKey(Long::sum)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // groupbykey version (kann sehr inpervomant sein)
//        sparkContext
//                .parallelize(inputData)
//                .mapToPair(rawValue ->
//                        new Tuple2<String, Long>(rawValue.split(":")[0], 1L)
//                )
//                .groupByKey()
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

    }

    public static void integerExample(JavaSparkContext sparkContext) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(22);
        inputData.add(12);
        inputData.add(34);


        JavaRDD<Integer> myRdd = sparkContext.parallelize(inputData);

        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        sqrtRdd.foreach(value -> System.out.println(value));


    }
}
