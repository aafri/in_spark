package com.in.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 对中间结果的Map结果进行汇�?，因Map过程已将Key进行排序，故后续只需要将不同的Key个数相加即可
 * @author Tzl
 * 
 */
public class LogReversedReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key,Iterable<Text> values,Context context)
			throws IOException,InterruptedException{
		long ip = 0;
		long counts = 0;
		for (Text value : values) {		
			String[] f = value.toString().split("\\s+");
			if(f.length == 2){
				counts += Long.parseLong(f[0]);
				ip += Long.parseLong(f[1]);
			}
		}
		context.write(key, new Text(counts+"\t"+ip));
	}
}
