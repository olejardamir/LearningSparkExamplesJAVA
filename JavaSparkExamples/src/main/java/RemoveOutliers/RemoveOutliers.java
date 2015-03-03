/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */

package RemoveOutliers;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

public class RemoveOutliers {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "basicmap",System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaDoubleRDD input = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0,.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1000.0));
		JavaDoubleRDD result = removeOutliers(input);
		System.out.println(StringUtils.join(result.collect(), ","));
		sc.stop();
	}

	static JavaDoubleRDD removeOutliers(JavaDoubleRDD rdd) {
		final StatCounter summaryStats = rdd.stats();
		final Double stddev = Math.sqrt(summaryStats.variance());
		return rdd.filter(new Function<Double, Boolean>() {
			public Boolean call(Double x) {
				return (Math.abs(x - summaryStats.mean()) < 3 * stddev);
			}
		});
	}
}