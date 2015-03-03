/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

public class BasicMapToDouble {
	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "basicmaptodouble", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaDoubleRDD result = getResult(rdd);
		System.out.println(StringUtils.join(result.collect(), ","));
	}
	
	private static JavaDoubleRDD getResult(JavaRDD<Integer> rdd){
		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer x) {
				double y = (double) x;
				return y * y;
			}
		});
		return result;
	}
}