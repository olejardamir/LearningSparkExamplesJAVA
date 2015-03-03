/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 03 2015. 
 */


package PerKeyAvg;

import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class PerKeyAvg {


	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "PerKeyAvg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		List<Tuple2<String, Integer>> input = new ArrayList();
		input.add(new Tuple2("coffee", 1));
		input.add(new Tuple2("coffee", 2));
		input.add(new Tuple2("pandas", 3));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		CalculateResult calculateResult = new CalculateResult();
		calculateResult.AvgCount(rdd);
		sc.stop();
	}
	
	
}