package com.in.mapred;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner of logstat
 * @author tzl
 *
 */
public class LogPartitioner extends Partitioner<LogAggrKey, LogAggrValue> {

	@Override
	/**
	 * We partition the data of mapper output only with the first part of LogAggrKey
	 */
	public int getPartition(LogAggrKey key, LogAggrValue value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.first().hashCode() & Integer.MAX_VALUE) % numPartitions;  
	}

}
