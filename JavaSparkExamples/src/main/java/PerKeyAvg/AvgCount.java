/*
 * Refactored Java Spark examples for easer understanding, to accompany "Learning spark lightning-fast big data analytics - O'Reilly Media"
 * @author: Damir Olejar, on March 02 2015. 
 */



package PerKeyAvg;

import java.io.Serializable;

public class AvgCount implements Serializable {

	protected int total_;
	protected int num_;
	
	/**
	 * The consturctor for the Average Count
	 * @param total
	 * @param num
	 */
	public AvgCount(int total, int num) {
		total_ = total;
		num_ = num;
	}

	/**
	 * Increments the total by x
	 * @param x
	 */
	public void incrementTotal(Integer x) {
		this.total_ += x;
	}

	
	/**
	 * Increments the number by x
	 * @param x
	 */
	public void incrementNum(Integer x) {
		this.num_ += x;
	}

	
	/**
	 * Gets the total_ variable
	 * @return
	 */
	public int getTotal() {
		return this.total_;
	}

	
	/**
	 * Gets the num_ variable
	 * @return
	 */
	public int getNum() {
		return this.num_;
	}
	
	
	/**
	 * Calculates the average
	 * @return
	 */
	public float avg() {
		return total_ / (float) num_;
	}

}
