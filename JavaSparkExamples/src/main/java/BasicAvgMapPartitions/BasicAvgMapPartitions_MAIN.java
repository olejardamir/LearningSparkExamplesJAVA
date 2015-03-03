/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 03 2015. 
 */

package BasicAvgMapPartitions;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class BasicAvgMapPartitions_MAIN {

public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "basicavgmappartitions", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		AvgCount result = new CalculateResult().getResult(rdd);
		System.out.println(result.avg());
		sc.stop();
	}

}
