package com.in.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Log Recuder to addup the count and uniq count of target
 * @author tzl
 *
 */
public class LogReducer extends Reducer<LogAggrKey, LogAggrValue, Text, Text> {

	/**
	 * Reduce the data
	 */
	public void reduce(LogAggrKey key,Iterable<LogAggrValue> values,Context context)
	throws IOException,InterruptedException{
		long count = 0L;
		long uniqCount = 0L;
		
		String lastTarget = null;
		for (LogAggrValue value : values){
			//read the target and cnt from value
			String 	target	= value.getTarget().toString();
			long	cnt 	= value.getCount().get();
			
			//add up the count
			count += cnt;
			if (lastTarget == null || !target.equals(lastTarget))
			{
				//it's a new target, add up uniq count
				uniqCount ++;
				lastTarget = target;
			}
		}
		//write out the result at the end of one group, data grouping is done by LogGroupComparator which ensure the 
		//data of same target is in one group
		context.write(key.first(), new Text(count + "\t"+ uniqCount));
	}
}
