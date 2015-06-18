package main.com.in.hbase;

import com.in.hbase.*;
import com.in.hbase.ReadHbaseWorkdayMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;




public class ReadHbaseWIFIJob extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
		int res = ToolRunner.run(new ReadHbaseWIFIJob(), args);
		System.exit(res);
		// TODO Auto-generated method stub
		/*Configuration conf = HBaseConfiguration.create();  
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if (otherArgs.length != 3) {  
            System.err.println("Usage: <tableName> <inputDir> <outputDir>");  
            System.exit(2);  
        }  
        HTable table = new HTable(conf, otherArgs[0]);  
        Job job = new Job((JobConf) conf);  
        ((JobConf) job).setJarByClass(HBaseBulkLoadDemo.class);  
        job.setJobName("HBaseBulkLoadDemo " + new Date());  
        job.setMapperClass(BulkLoadDemoMapper.class);  
        job.setReducerClass(PutSortReducer.class);  
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
        job.setMapOutputValueClass(Put.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));  
        HFileOutputFormat.configureIncrementalLoad(job, table);  
        return job.waitForCompletion(true) ? 0 : 1;  */
	}
	
@SuppressWarnings("deprecation")
public int run(String args[]) throws Exception {
		//women xian zhijie put dao hbase zhong 
	//we put data into hbase first
		if (args.length != 2 && args.length != 3) {
			System.out.println("Params error.");
			System.exit(-1);
		}
		//Configuration hconf = HBaseConfiguration.create();  
		 Configuration conf = new Configuration();
		//HTable table=new HTable(hconf, args[1]);  
		Job job = Job.getInstance(getConf(), "ReadHbaseJob");
		job.setJarByClass(ReadHbaseWIFIJob.class);
		FileSystem fs=FileSystem.get(conf);
		//fs.getChildFileSystems();
		//fs.l
		Path listf =new Path("/lib/hbase");
        //fs.listFiles(listf,true );     
		//FileSystem hdfs=FileSystem.get(conf);
        FileStatus stats[]=fs.listStatus(listf);
        for(int i = 0; i < stats.length; ++i){
    		job.addFileToClassPath(stats[i].getPath());
    		}	
        //FileStatus stats[]=hdfs.listStatus(listf);
		Path p=new Path("/lib/hbase-common-0.98.8-hadoop2.jar");
		//job.addArchiveToClassPath(p);
		job.setNumReduceTasks(12);
//		TextInputFormat.addInputPath(job, new Path(args[0]));

    	job.getConfiguration().setBoolean("mapred.compress.map.output", true);
		job.getConfiguration().setClass("mapreduce.map.output.compress.codec",
				LzoCodec.class, CompressionCodec.class);
		
//		LzoTextInputFormat.addInputPath(job, new Path(args[0]));
//		job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
		
		//HDFSUtil.addDailyLzoInputPaths(job, args[0], args[2], 60);
		//0 path,1 tablename
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setMapperClass(TugoGelUsers.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(LongWritable.class);
		// job.setMapperClass(ReadHbaseMapper.class);  
		 //job.setReducerClass(ReadHbaseReducer.class);
		// job.setInputFormatClass(TextInputFormat.class);
		 //shengcheng hfile
		job.getConfiguration().setInt("mapreduce.map.memory.mb", 2046);
	     job.setMapOutputKeyClass(Text.class);  
	     job.setMapOutputValueClass(Text.class);  
	    // job.setMapOutputValueClass(Put.class);
	    // HTable table = new HTable(hconf, args[1]); 
	     if(args.length==3 && args[2]=="workday"){
		   job.setMapperClass(main.com.in.hbase.ReadHbaseWorkdayMapper.class);
	     }else if(args.length==3 && args[2]=="weekend"){
	 		job.setMapperClass(ReadHbaseWeekendMapper.class);

	     }else{
		 		job.setMapperClass(com.in.hbase.ReadHbaseMapper.class);

	     }
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	     //HFileOutputFormat2.c
		// HFileOutputFormat2..configureIncrementalLoad(job, table);  
		// HFileOutputFormat2.configureIncrementalLoadMap(job, table);
		// TableMapReduceUtil.initTableReducerJob(args[1], null, job);
		 //TableMapReduceUtil.init
		 //org.apache.hadoop.hbase.mapreduce.
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
