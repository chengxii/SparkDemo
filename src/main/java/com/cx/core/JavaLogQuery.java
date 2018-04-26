package com.cx.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 执行针对Apache日志的滚动样式查询。
 * @author xi.cheng
 */
public final class JavaLogQuery {

    public static final List<String> exampleApacheLogs = Arrays.asList(
            "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                    "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                    ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" " +
                    "62.24.11.25 images.com 1358492167 - Whatup",
            "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg " +
                    "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; " +
                    "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR " +
                    "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR  " +
                    "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" " +
                    "0 73.23.2.15 images.com 1358492557 - Whatup");

    public static final Pattern apacheLogRegex = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("JavaLogQuery")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> dataSet = (args.length == 1) ? jsc.textFile(args[0]) : jsc.parallelize(exampleApacheLogs);

        JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet.mapToPair(line -> new Tuple2<>(extractKey(line), extractStats(line)));
        JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(Stats::merge);

        List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
        for (Tuple2<Tuple3<String, String, String>, Stats> t : output) {
            System.out.println(t._1() + "\t" +t._2());
        }
        sparkSession.stop();
    }

    private static Stats extractStats(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            int bytes = Integer.parseInt(m.group(7));
            return new Stats(1, bytes);
        } else {
            return new Stats(1, 0);
        }
    }

    private static Tuple3<String, String, String> extractKey(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            String user = m.group(3);
            String query = m.group(5);
            if (!"-".equalsIgnoreCase(user)) {
                return new Tuple3<>(ip, user, query);
            }
        }
        return new Tuple3<>(null, null, null);
    }

    /** 跟踪一个特定组的总查询数和总字节数 */
    public static class Stats implements Serializable {

        private final int count;
        private final int numBytes;

        public Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }
        public Stats merge(Stats other) {
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        @Override
        public String toString() {
            return String.format("bytes=%s\tn=%s", numBytes, count);
        }
    }
}
