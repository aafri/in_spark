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
import com.in.mapred.ResponseReducer;
import com.in.mapred.ResponseValue;
import com.in.util.HDFSUtil;




/**
          统计项：
          总访问次数（所有日志），总响应时间，平均响应时间
           错误访问数（状态码错误日志）--4XX 5XX，总的响应时间，平均响应时间
   /app/paster/leftcount  
          需要加载的jar文件通过 -libjar参数来制定，-files参数指定需要上传的配置文件，同时在命令行之外添加额外的参数以支持更多的时间区域的选择     
 */

public class INAPPJob extends Configured implements Tool {

	public INAPPJob() {
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new INAPPJob(), args);
		System.exit(res);
	}

	public int run(String args[]) throws Exception {
		
		if (args.length != 4) {
			System.out.println("Params error.");
			System.exit(-1);
		}
		
		Job job = Job.getInstance(getConf(), "InAppJob");
		job.setJarByClass(INAPPJob.class);
		
		job.setNumReduceTasks(4);

//		TextInputFormat.addInputPath(job, new Path(args[0]));

    	job.getConfiguration().setBoolean("mapred.compress.map.output", true);
		job.getConfiguration().setClass("mapred.map.output.compression.codec",
				LzoCodec.class, CompressionCodec.class);
		
//		LzoTextInputFormat.addInputPath(job, new Path(args[0]));
//		job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		
		HDFSUtil.addDailyLzoInputPaths(job, args[0], args[2], 0, args[3]);
		
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(INAPPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResponseValue.class);
//		job.setCombinerClass(ResponseCombiner.class);
		job.setReducerClass(ResponseReducer.class);
//		job.setPartitionerClass(LogPartitioner.class);
//		job.setGroupingComparatorClass(LogGroupComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
