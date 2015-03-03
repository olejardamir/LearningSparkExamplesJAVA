/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 03 2015. 
 */


package BasicAvgMapPartitions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

/**
 * The purpose of this class is to calculate the result using the Spark's parallel computing
 *
 */

public class CalculateResult implements Serializable{
	
	
	/**
	 * Gets the result as a AvgCount object
	 * @param rdd
	 * @return
	 */
	public AvgCount getResult(JavaRDD<Integer> rdd){
 		Function2<AvgCount, AvgCount, AvgCount> combine = getCombine();
 		FlatMapFunction<Iterator<Integer>, AvgCount> setup = getSetup();
		AvgCount result = rdd.mapPartitions(setup).reduce(combine);
		return result;	
	}
	
	
 
	
	/**
	 * A Spark Function class meant for incrementing the split AvgCounts 
	 * It accepts the two AvgCounts and returns a combined(incremented) AvgCount
	 * @return
	 */
	private  Function2<AvgCount, AvgCount, AvgCount> getCombine(){
		Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			public AvgCount call(AvgCount a, AvgCount b) {
				a.incrementTotal(b.getTotal());
				a.incrementNum(b.getNum());
				return a;
			}
		};
		return combine;	
	}
	
	/**
	 * A FlatMap Function
	 * @return
	 */
	private FlatMapFunction<Iterator<Integer>, AvgCount> getSetup(){
	FlatMapFunction<Iterator<Integer>, AvgCount> setup = new FlatMapFunction<Iterator<Integer>, AvgCount>() {
		public Iterable<AvgCount> call(Iterator<Integer> input) {
			AvgCount a = new AvgCount(0, 0);
			while (input.hasNext()) {
				a.incrementTotal(input.next());
				a.incrementNum(1);
			}
			ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
			ret.add(a);
			return ret;
		}
	};
	return setup;
	}
	
}
