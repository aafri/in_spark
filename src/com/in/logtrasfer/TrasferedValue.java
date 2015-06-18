package com.in.logtrasfer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TrasferedValue implements Writable {
	Text oldLogValue;
	String action;
	Text url;
    Map<String, String> requestMetricsMap = new TreeMap<String, String>();
	
    public TrasferedValue genTrasferedValue(){
    	TrasferedValue trasferedValue=new TrasferedValue();
    	return trasferedValue;
    }
	public static final String LOGITEM_URL = "url";
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
        
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
        
	}

}
