package BasicMap;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class BasicMap {
	public static void main(String[] args) throws Exception {
 
		JavaSparkContext sc = new JavaSparkContext("local", "basicmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = getResult(rdd);
		System.out.println(StringUtils.join(result.collect(), ","));
		sc.stop();
	}
	
	
	private static JavaRDD<Integer> getResult(JavaRDD<Integer> rdd){
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			public Integer call(Integer x) {
				return x * x;
			}
		});
		return result;
		
	}
	
}
