package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	// 直方图的桶
	private int[] bucketArray;
	private int min;
	private int max;
	private double widthOfBucket;
	private int tupleCount;
	/**
	 * Create a new IntHistogram.
	 * <p>
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * <p>
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * <p>
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed.  For example, you shouldn't
	 * simply store every value that you see in a sorted list.
	 *
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		this.bucketArray = new int[buckets];
		this.min = min;
		this.max = max;
		this.widthOfBucket = (max - min + 1.0) / buckets;
		tupleCount = 0;
	}

	private int getIndex(int v) {
		return  (int) ((v - min) / widthOfBucket);
	}
	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		if (v > max || v < min) {
			return;
		}
		tupleCount += 1;
		int index = getIndex(v);
		bucketArray[index] += 1;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * <p>
	 * For example, if "op" is "GREATER_THAN" and "v" is 5,
	 * return your estimate of the fraction of elements that are greater than 5.
	 *
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		int index = getIndex(v);
		switch (op) {
			case EQUALS: {
				if (index < 0 || index >= bucketArray.length) {
					return 0.0;
				}
				return bucketArray[index] / (tupleCount * 1.0);
			}
			case LESS_THAN: {
				if (index >= bucketArray.length) {
					return 1.0;
				}
				int count = 0;
				for (int i = 0; i < index; i++) {
					count += bucketArray[i];
				}
				return count / (tupleCount * 1.0);
			}
			case LESS_THAN_OR_EQ: {
				if (index >= bucketArray.length) {
					return 1.0;
				}
				int count = 0;
				for (int i = 0; i <= index; i++) {
					count += bucketArray[i];
				}
				return count / (tupleCount * 1.0);
			}
			case GREATER_THAN: {
				if (index < 0) {
					return 1.0;
				}
				int count = 0;
				for (int i = index + 1; i < bucketArray.length; i++) {
					count += bucketArray[i];
				}
				return count / (tupleCount * 1.0);
			}
			case GREATER_THAN_OR_EQ: {
				if (index < 0) {
					return 1.0;
				}
				int count = 0;
				for (int i = index; i < bucketArray.length; i++) {
					count += bucketArray[i];
				}
				return count / (tupleCount * 1.0);
			}
			case NOT_EQUALS: {
				if (index < 0 || index >= bucketArray.length) {
					return 1.0;
				}
				return (tupleCount - bucketArray[index]) / (tupleCount * 1.0);
			}
		}
		return 0.0;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * <p>
	 * This is not an indispensable method to implement the basic
	 * join optimization. It may be needed if you want to
	 * implement a more efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here
		return 1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	@Override
	public String toString() {
		return String.format("widthOfBucket: %f", widthOfBucket);
	}
}
