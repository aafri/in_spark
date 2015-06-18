package com.in.mapred;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;;
/**
 * The key Comparator of Reducer which do data grouping
 * It group LogAggrKey with the first part only
 * @author tzl
 *
 */
public class LogGroupComparator extends WritableComparator {

	protected LogGroupComparator() {
		super(LogAggrKey.class, true);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Compare the keys and do grouping
	 * We only use the first part of aggregated keys to do grouping in reducer side
	 */
	public int compare(WritableComparable key1, WritableComparable key2)
	{	
		LogAggrKey a = (LogAggrKey) key1;
		LogAggrKey b = (LogAggrKey) key2;
		return a.first().compareTo(b.first());
	}


}
