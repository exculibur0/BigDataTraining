package org.led.bd.word_count_spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class MainApp 
{
	public static void countWord(String inputFile, String outputFile) {
		SparkConf conf = new SparkConf().setAppName("CountRecord");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, String> logs = sc.wholeTextFiles(inputFile).cache();
		
		//========================flatMapValue work with new Function (not FlatMapFunction)======
		JavaPairRDD<String, String> lineLog = logs.flatMapValues(new Function<String, Iterable<String>>() {
			public Iterable<String> call(String arg0) throws Exception {
				String[] lines = arg0.split("\n");
				return Arrays.asList(lines);
			}
		});
		
		JavaPairRDD<String, String> wordLog = lineLog.flatMapValues(new Function<String, Iterable<String>>() {
			public Iterable<String> call(String arg0) throws Exception {
				String[] items = arg0.trim().split(" ");
				return Arrays.asList(items);
			}
		});
		
		JavaPairRDD<String, String> fullWordLog = wordLog.filter(new Function<Tuple2<String, String>, Boolean>() {
			public Boolean call(Tuple2<String, String> arg0) throws Exception {
				if ((arg0._2() != null) && (arg0._2().equals("with")) || (arg0._2().equals("up")) || (arg0._2().equals("who"))) {
					return true;
				} else {
					return false;
				}
			}			
		});
		
		JavaPairRDD<String, Integer> wordList = fullWordLog.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>(){
			public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String, Integer> (t._2(), new Integer(1));
			}			
		});
		
		JavaPairRDD<String, Integer> keyRed = wordList.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}			
		});
		
		keyRed.saveAsTextFile(outputFile);
		
	}
    public static void main( String[] args )
    {
    	String input = "/input/novel.txt";
    	String output = "/output/result_spark.txt";
    	countWord(input, output);
        
    }
}
