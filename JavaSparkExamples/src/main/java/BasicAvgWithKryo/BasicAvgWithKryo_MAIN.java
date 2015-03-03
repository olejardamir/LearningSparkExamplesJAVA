/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */


package BasicAvgWithKryo;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import BasicAvg.AvgCount;
import BasicAvg.CalculateResult;



/**
* The purpose of this main class is to initiate the SparkContext and then to calculate the average of the array (1,2,3,4).
* For calculating, it uses the Calculate Result class as well as the Average Count class.
* Everything is done on a local host
* @param args
* @throws Exception
*/


public final class BasicAvgWithKryo_MAIN {
	// This is our custom class we will configure Kyro to serialize

	public static void main(String[] args) throws Exception {

		//Initiate the spark context on a local machine, with the name basic avg.
		SparkConf conf = new SparkConf().setMaster("local").setAppName("basicavgwithkyro");
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", AvgRegistrator.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
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
