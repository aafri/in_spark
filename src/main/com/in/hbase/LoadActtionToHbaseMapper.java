package main.com.in.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadActtionToHbaseMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		int action=0;
		if (values.length == 6) {		  
			  if (values[2].equals("view")){
				  action=1;
			  }else if(values[2].equals("love")){
				  action=2;
			  }else if(values[2].equals("share")){
				  action=3;
			  }else if(values[2].equals("collection")){
				  action=4;
			  }else if(values[2].equals("download")){
				  action=5;
			  }else if(values[2].equals("poke")){
				  action=6;
			}			  
			byte[] rkey = Bytes.toBytes(values[0]+"_"+action+"_"+values[1]); // rowkey			
			byte[] family = Bytes.toBytes("f"); // 列族
			byte[] column_name = Bytes.toBytes("t"); // 列
			// token ip wifi time iswork x y
			// 0000f804a03ef7f46f3e190293760b4 column=f:201501011225,
			// timestamp=1423144939465,
			// value=WIFI,0,116.929883,36.614505,222.161.250.160
			byte[] colume_value = Bytes.toBytes(com.in.util.TimeUtil.getLongDate(values[5])); // 值
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rkey);
			Put put = new Put(rkey); // hbase format data--key
			put.add(family, column_name, colume_value); // hbase format data--value
			context.write(rowKey, put);
		}
	}
}