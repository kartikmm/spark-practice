package com.practice.spark.Tuples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		final List<Integer> inputData = new ArrayList<>();

		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

		final SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		final JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

		//Not to do as below, instead use Tuple
//		final JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
		final JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));
		
		sc.close();
	}

}
