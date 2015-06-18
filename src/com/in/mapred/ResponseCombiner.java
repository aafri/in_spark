package com.in.mapred;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ResponseCombiner extends Reducer<Text, ResponseValue, Text, ResponseValue>{
	
	/**
	 * Reduce the mapper output
	 */
	public void reduce(Text key,Iterable<ResponseValue> values,Context context)
			throws IOException,InterruptedException{
		
		double count = 0L;
		String lastTarget = null;
		
		for (ResponseValue value : values){
			String 	target	= value.getTarget().toString();
			double	cnt 	= value.getCount().get();
			
			//add up the count of target
			if (lastTarget != null && !target.equals(lastTarget))
			{
				this.output(context, key, lastTarget, count);
				count = 0L;
			}
			lastTarget = target;
			count += cnt;			
		}
		
		if (lastTarget != null)
		{
			this.output(context, key, lastTarget, count);
		}
	}
	
	/**
	 * Output the combined result
	 * @param context
	 * @param key
	 * @param target
	 * @param count
	 * @throws java.io.IOException
	 * @throws InterruptedException
	 */
	private void output(Context context, Text key, String target, double count)
		throws IOException,InterruptedException
	{
		Text outTarget = new Text(target);
		context.write(new Text(key), 
								new ResponseValue(outTarget, new DoubleWritable(count)
								)
		);
	}

}
