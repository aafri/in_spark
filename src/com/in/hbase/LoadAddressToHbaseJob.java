package com.in.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

public class LoadAddressToHbaseJob extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new LoadAddressToHbaseJob(), args);

	}

	@SuppressWarnings("deprecation")
	public int run(String args[]) throws Exception {
		// we put data into hbase first
		if (args.length != 2) {
			System.out.println("Params error.");
			System.exit(-1);
		}
		Configuration hconf = HBaseConfiguration.create();
		// HTable table=new HTable(hconf, args[1]);
		Job job = Job.getInstance(getConf(), "LoadAddressJob");
		job.setJarByClass(LoadAddressToHbaseJob.class);
		job.setNumReduceTasks(12);// reduce action do put data into hbase ,the
									// more the better
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
		job.getConfiguration().setClass("mapreduce.map.output.compress.codec",
				LzoCodec.class, CompressionCodec.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Configuration conf = new Configuration();
		FileSystem fs=FileSystem.get(conf);
		Path listf =new Path("/lib/mapred");
	    FileStatus stats[]=fs.listStatus(listf);
	    for(int i = 0; i < stats.length; ++i){
	    	job.addFileToClassPath(stats[i].getPath());
	    }
		job.setMapperClass(LoadActtionToHbaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		TableMapReduceUtil.initTableReducerJob(args[1], null, job);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
