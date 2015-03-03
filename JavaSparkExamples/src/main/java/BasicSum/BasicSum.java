/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */

package BasicSum;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class BasicSum {
	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "basicmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		Integer result = getResult(rdd);
		System.out.println(result);
		sc.stop();
	}
	
	private static Integer getResult(JavaRDD<Integer> rdd){
		Integer result = rdd.fold(0,
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});
		return result;
	}
	
}