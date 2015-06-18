
package com.in.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogReversedMapper extends Mapper<LongWritable, Text, Text, Text> {

	class Counter {
		
		Set<String> ip = new HashSet<String>();
		long counts = 0;
		
		public Counter(String ip, long counts) {
			// TODO Auto-generated constructor stub
			this.ip.add(ip);
			this.counts += counts;
		}
		
		public void add(String ip, long counts) {
			// TODO Auto-generated method stub
			this.ip.add(ip);
			this.counts += counts;
		}
	}
	
	Map<String,Counter> map = new HashMap<String,Counter>();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] f = value.toString().split("\\s+");
		if(map.containsKey(f[1])){
			map.get(f[1]).add(f[0],Long.parseLong(f[2]));
		}else{
			map.put(f[1], new Counter(f[0],Long.parseLong(f[2])));
		}

	}
	
	public void cleanup(Context context){
		for(String key : map.keySet()){
			try {
				context.write(new Text(key), new Text(map.get(key).counts+"\t"+map.get(key).ip.size()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
