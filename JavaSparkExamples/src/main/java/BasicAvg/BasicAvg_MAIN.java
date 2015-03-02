/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */



package BasicAvg;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;



public final class BasicAvg_MAIN {
	
	
	/**
	 * The purpose of this main class is to initiate the SparkContext and then to calculate the average of the array (1,2,3,4).
	 * For calculating, it uses the Calculate Result class as well as the Average Count class.
	 * Everything is done on a local host
	 * @param args
	 * @throws Exception
	 */
	
	public static void main(String[] args) throws Exception {
		//Initiate the spark context on a local machine, with the name basic avg.
		JavaSparkContext sc = new JavaSparkContext("local", "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		
		//Create a new RDD object paralelizing the array 1,2,3,4
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		
		//Initiate the calculate object and get the AvgCount for getting the final result
		CalculateResult calculate = new CalculateResult();
		AvgCount result = calculate.getResult(rdd);
		
		//display the result
		System.out.println(result.avg());
		
		//stop the SparkContext
		sc.stop();
	}	
	
}