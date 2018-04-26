package com.cx;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author xi.cheng
 */
public class JavaWordCountPro {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaWordCount")
                .getOrCreate();

        String path = "src/main/file/words";
        JavaRDD<String> lines = spark.read().textFile(path).javaRDD();
        // JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(SPACE.split(line)).iterator());
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

        // JavaPairRDD<String, Integer> wordTuple = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordTuple = words.mapToPair(word -> new Tuple2<>(word, 1));

        // JavaPairRDD<String, Integer> count = wordTuple.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        JavaPairRDD<String, Integer> count = wordTuple.reduceByKey((v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> outpue = count.collect();
        for (Tuple2<String, Integer> tuple2 : outpue) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }
        spark.stop();
    }
}
