/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 03 2015. 
 */

package IntersectByKey;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Iterables;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public final class IntersectByKey {


	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local", "IntersectByKey",System.getenv("SPARK_HOME"), System.getenv("JARS"));
		List<Tuple2<String, Integer>> input1 = new ArrayList();
		List<Tuple2<String, Integer>> input2 = new ArrayList();
		
		input1.add(new Tuple2("coffee", 1));
		input1.add(new Tuple2("coffee", 2));
		input1.add(new Tuple2("pandas", 3));
		input2.add(new Tuple2("pandas", 20));
		
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(input1);
		JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(input2);
		JavaPairRDD<String, Integer> result = intersectKeys(rdd1, rdd2);
		
		printResults(result);
		sc.stop();
	}

	
	
	private static <K, V> JavaPairRDD<K, V> intersectKeys(JavaPairRDD<K, V> rdd1, JavaPairRDD<K, V> rdd2) {
		JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<V>>> grouped = rdd1.cogroup(rdd2);
		
		JavaPairRDD<K, V> jpair = grouped.flatMapValues(new Function<Tuple2<Iterable<V>, Iterable<V>>, Iterable<V>>() {
			public Iterable<V> call(Tuple2<Iterable<V>, Iterable<V>> input) {
				
				ArrayList<V> al = new ArrayList<V>();
				if (!Iterables.isEmpty(input._1())&& !Iterables.isEmpty(input._2())) {
					Iterables.addAll(al, input._1());
					Iterables.addAll(al, input._2());
				}
				
				return al;
			}
			
		});
		
		return jpair;
	}
	
	
	
	private static void printResults(JavaPairRDD<String, Integer> result){
		for (Tuple2<String, Integer> entry : result.collect()) {
			System.out.println(entry._1() + ":" + entry._2());
		}
		System.out.println("Done");
	}
	
	
	
}
