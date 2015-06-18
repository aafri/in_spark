package main.com.in.mapred;

import java.io.IOException;


import com.in.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Combiner for LogAggrKey, LogAggrValue outputted by mappper
 * @author tzl
 *
 */
public class LogCombiner extends Reducer<com.in.mapred.LogAggrKey, LogAggrValue, com.in.mapred.LogAggrKey, LogAggrValue> {

	/**
	 * Reduce the mapper output
	 */
	public void reduce(com.in.mapred.LogAggrKey key,Iterable<LogAggrValue> values,Context context)
			throws IOException,InterruptedException{
		
		long count = 0L;
		String lastTarget = null;
		
		for (LogAggrValue value : values){
			String 	target	= value.getTarget().toString();
			long	cnt 	= value.getCount().get();
			
			//add up the count of target
			if (lastTarget != null && !target.equals(lastTarget))
			{
				this.output(context, key, lastTarget, count);
				count = 0;
			}
			lastTarget = target;
			count += cnt;			
		}
		
		if (lastTarget != null)
		{
			// ������Ϊ����Ӧ��ͳ����-Key���ֵĴ���-ȥ�غ�Key�ĸ���
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
	private void output(Context context, com.in.mapred.LogAggrKey key, String target, long count)
		throws IOException,InterruptedException
	{
		Text outTarget = new Text(target);
		context.write(new com.in.mapred.LogAggrKey(
								key.first(), outTarget), 
								new LogAggrValue(outTarget, new LongWritable(count)
								)
		);
	}
}
