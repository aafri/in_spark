package com.in.upv;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.in.mapred.LogAggrKey;
import com.in.mapred.LogAggrValue;
import com.in.mapred.LogGroupComparator;
import com.in.mapred.LogPartitioner;
import com.in.mapred.LogReducer;

public class UPVJob extends Configured implements Tool {

	public UPVJob() {
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new UPVJob(), args);
		System.exit(res);
	}

	public int run(String args[]) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Params error.");
			System.exit(-1);
		}
		
		Job job = Job.getInstance(getConf(), "UPVJob");

		job.setJarByClass(UPVJob.class);
		
		job.getConfiguration().setStrings("mapred.child.java.opts",
				new String[] { "-Xmx768M" });
		
    	job.getConfiguration().setBoolean("mapred.compress.map.output", true);
		job.getConfiguration().setClass("mapred.map.output.compression.codec",
				LzoCodec.class, CompressionCodec.class);
		
		LzoTextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
        job.setMapperClass(UPVMapper.class);
        
		job.setMapOutputKeyClass(LogAggrKey.class);
		job.setMapOutputValueClass(LogAggrValue.class);

		job.setReducerClass(LogReducer.class);	
		job.setPartitionerClass(LogPartitioner.class);
		job.setGroupingComparatorClass(LogGroupComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
