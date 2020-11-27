package com.practice.spark.Mapping;

import com.practice.spark.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

        Util.setSystemProperties();

        final List<Integer> inputData = new ArrayList<>();

		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

		final SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		final JavaSparkContext sc = new JavaSparkContext(conf);

		final JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		final Integer result = myRdd.reduce((value1, value2 ) -> value1 + value2 );

		System.out.println("Reduce Result = " + result);

		final JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
		sqrtRdd.collect().forEach(System.out::println);

		// how many elements in sqrtRdd
		// using just map and reduce
		final JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
		final Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println("Count = " + count);
		
		sc.close();
	}

}
