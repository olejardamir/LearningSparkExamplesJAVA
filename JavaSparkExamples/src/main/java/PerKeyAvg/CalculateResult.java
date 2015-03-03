/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 03 2015. 
 */


package PerKeyAvg;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
	public void AvgCount(JavaPairRDD<String, Integer> rdd){
		Function<Integer, AvgCount> createAcc = getCreateAcc();
		Function2<AvgCount, Integer, AvgCount> addAndCount = getAddAndCount();
		Function2<AvgCount, AvgCount, AvgCount> combine = getCombine();
 
		AvgCount initial = new AvgCount(0, 0);
		JavaPairRDD<String, AvgCount> avgCounts = rdd.combineByKey(createAcc,addAndCount, combine);
		Map<String, AvgCount> countMap = avgCounts.collectAsMap();
		displayEntries(countMap);
	}
	
	/**
	 * Displays the countMap
	 * @param countMap
	 */
	private void displayEntries(Map<String, AvgCount> countMap) {
		for (Entry<String, AvgCount> entry : countMap.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().avg());
		}
		System.out.println("Done");
	}
	
	/**
	 * A Spark Function class meant for incrementing the total_ and a num_
	 * It accepts the AvgCount and Integer and returns the AvgCount
	 * @return
	 */
	private  Function2<AvgCount, Integer, AvgCount> getAddAndCount(){
		Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
			public AvgCount call(AvgCount a, Integer x) {
				a.incrementTotal(x);
				a.incrementNum(1);
				return a;
			}
		};
		return addAndCount;
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
	 * A Spark Function class meant for creating an average count call
	 * @return
	 */
	private static Function<Integer, AvgCount> getCreateAcc(){
	Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
		public AvgCount call(Integer x) {
			return new AvgCount(x, 1);
		}
	};
	return createAcc;
	}
	
	
}
