package com.in.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.in.util.Consnt;
import com.in.util.HDFSUtil;

/**
 * MapReduce程序启动类，通过继承Configured类并实现Tool接口来扩展命令行参数模式。
 * 
 * @author tzl
 *
 */
public class LogStatNew extends Configured implements Tool{

	/**
	 * 使用ToolRunner来调用GenericOptionsParser
	 * 
	 * @param args 命令行参数
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new LogStatNew(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
        
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		// 根据配置文件中的job名称来设定任务名
		String jobName = conf.get("stat.job.name","GenericLogStat");
		job.setJobName(jobName);

		if(args.length != 1){
			System.err.println("Usage: "+job.getConfiguration().get("stat.main.class")+" [-Generic Options] <Date String>");
			System.exit(-1);
		}

		// 根据配置文件中的reduce任务数来设定
		String tasks = conf.get("stat.reduce.tasks","10");
		job.setNumReduceTasks(Integer.parseInt(tasks));

		job.setJarByClass(LogStatNew.class);

		// map输出结果使用lzo压缩
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);

		// reduce结果使用lzop压缩
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);

		conf.setStrings(Consnt.STAT_DATE_STR, args[0]);

		// 设定输入文件
		HDFSUtil.addInputPaths(job, getConf().get("stat.in.path"), args[0]);
				
		job.setInputFormatClass(LzoTextInputFormat.class);

		Path tmp = new Path(conf.get(Consnt.STAT_OUT_PATH)+"/"+args[0]);

		FileSystem.get(conf).delete(tmp, true);
		FileOutputFormat.setOutputPath(job, tmp);

		if(conf.get("stat.mapper.class") != null){
			job.setMapperClass((Class<? extends Mapper>) Class.forName(conf
					.get("stat.mapper.class")));
		}else{
			job.setMapperClass(LogMapperNew.class);
		}

		job.setMapOutputKeyClass(LogAggrKey.class);
		job.setMapOutputValueClass(LogAggrValue.class);
		job.setReducerClass(LogReducer.class);	
		job.setPartitionerClass(LogPartitioner.class);
		job.setGroupingComparatorClass(LogGroupComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;	
	}
}
