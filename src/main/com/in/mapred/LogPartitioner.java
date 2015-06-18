package main.com.in.mapred;

import com.in.mapred.*;
import com.in.mapred.LogAggrValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner of logstat
 * @author tzl
 *
 */
public class LogPartitioner extends Partitioner<com.in.mapred.LogAggrKey, com.in.mapred.LogAggrValue> {

	@Override
	/**
	 * We partition the data of mapper output only with the first part of LogAggrKey
	 */
	public int getPartition(com.in.mapred.LogAggrKey key, LogAggrValue value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.first().hashCode() & Integer.MAX_VALUE) % numPartitions;  
	}

}
