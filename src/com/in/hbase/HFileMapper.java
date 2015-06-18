package com.in.hbase;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class HFileMapper extends Mapper<LongWritable, Text,ImmutableBytesWritable,Put> { 
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		
		
		String[] values = value.toString().split("\t"); 
		if(values.length==8){
		byte[] rkey = Bytes.toBytes(values[0]);     //rowkey
		byte[] family = Bytes.toBytes("f");      //列族
		byte[] column_time = Bytes.toBytes(values[3]);      //列
		//token ip wifi time iswork x y 
		//0000f804a03ef7f46f3e190293760b4  column=f:201501011225, timestamp=1423144939465, value=WIFI,0,116.929883,36.614505,222.161.250.160
		byte[] val_time = Bytes.toBytes(values[2]+","+values[4]+","+values[5]+","+values[6]+","+ values[1]);      //值
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rkey);   
         Put put = new Put(rkey); //hbase format data--key
         put.add(family, column_time, val_time);  //hbase format data--value
         context.write(rowKey, put);  
		}
	}
} 