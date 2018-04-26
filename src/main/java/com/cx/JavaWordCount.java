package com.cx;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // if (args.length < 1) {
        //     System.err.println("Usage: JavaWordCount <file>");
        //     System.exit(1);
        // }

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaWordCount")
                .getOrCreate();

        // String path = JavaWordCount.class.getClassLoader().getResource("words").getFile();
        // JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        String path = "src/main/file/words";
        JavaRDD<String> lines = spark.read().textFile(path).javaRDD();
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(SPACE.split(line)).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordTuple = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> count = wordTuple.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> outpue = count.collect();
        for (Tuple2<String, Integer> tuple2 : outpue) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }
        spark.stop();
    }
}
