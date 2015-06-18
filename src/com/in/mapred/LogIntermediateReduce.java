package com.in.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.in.util.Counter;

public class LogIntermediateReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Counter> map = new HashMap<String, Counter>();
		for (Text value : values) {
			String[] fields = value.toString().split("\\s+");
			if (fields.length == 2) {
				if (map.containsKey(fields[0])) {
					map.get(fields[0]).plus(Long.parseLong(fields[1]));
				} else {
					map.put(fields[0], new Counter(Long.parseLong(fields[1])));
				}
			}
		}

		for (String statKey : map.keySet()) {
			context.write(key, new Text(statKey + "\t"
					+ map.get(statKey).getCnt()));
		}
	}
}
