package main.com.in.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadHbaseWIFIMapper extends Mapper<LongWritable, Text, Text, Text> {
	HTable table = null;
	Get get;
	Result result;

	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		String tablename = "gps";
		table = new HTable(conf, tablename);
	}

	/*
	 * private Pattern p =
	 * Pattern.compile("^GET /onepiece/itugo_deleven.html\\?"); private Pattern
	 * p1 = Pattern.compile("uid=([^&]+)");// uid=123 private Pattern p2 =
	 * Pattern.compile("aliwwid=([^& ]+)");
	 */
	// LongWritable sign1=new LongWritable(1);
	// LongWritable sign2=new LongWritable(4);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] lineList = line.split("\t");
		get = new Get(Bytes.toBytes(lineList[0]));
		result = table.get(get);
		byte[] qualifiername;
		byte[] cellvalue;
		// byte[] celltimestp;
		String stringvalue;
		String[] stringvaluelist;
		String x;
		String y;
		String isworkday;
		String qualifiernames;
		String dayflag;
		while (result.advance()) {
			Cell cell = result.current();
			cell.getFamily();
			qualifiername = cell.getQualifier();
			cellvalue = cell.getValue();
			// cell.getTimestamp();
			stringvalue = Bytes.toString(cellvalue);
			stringvaluelist = stringvalue.split(",");
			// dates=Bytes.toString(qualifiername).substring(0, 8);
			// try {
			if (stringvaluelist.length == 5) {
				qualifiernames = Bytes.toString(qualifiername);
				// if( DateTool.isWorkDay(dates)){
				dayflag = stringvaluelist[1];
				qualifiernames = Bytes.toString(qualifiername);
					x = stringvaluelist[2];
					y = stringvaluelist[3];
					context.write(new Text(lineList[0]), new Text(
							qualifiernames + "\t" + x + "\t" + y + "\t"
									+ stringvaluelist[0] + "\t"
									+ stringvaluelist[4]+"\t"+dayflag));
					//token time x y  wifi Ip iswork
				
			}

		}

	}

	public static void main(String[] args) throws IOException {
		HTable table = null;
		Get get;
		Result result;
		Configuration conf = HBaseConfiguration.create();
		String tablename = "gps";
		table = new HTable(conf, tablename);
		get = new Get(Bytes.toBytes("0000a6272407e1fa352e74c751ff9828"));
		result = table.get(get);
		// System.out.println(result);

		byte[] qualifiername;
		byte[] cellvalue;
		// byte[] celltimestp;
		String stringvalue;
		String[] stringvaluelist;
		String x;
		String y;
		while (result.advance()) {
			Cell cell = result.current();
			// System.out.println(cell);
			cell.getFamily();
			qualifiername = cell.getQualifier();
			System.out.println(Bytes.toString(qualifiername));
			cellvalue = cell.getValue();
			stringvalue = Bytes.toString(cellvalue);
			stringvaluelist = stringvalue.split(",");
			System.out.println(stringvaluelist.length);
			// if(stringvaluelist.length==2||stringvaluelist.length==3){
			x = stringvaluelist[0];
			y = stringvaluelist[1];
			// System.out.println("8e24bebf0087c77190a2353f0d157216"+"\t"+Bytes.toString(qualifiername)+"\t"+x+"\t"+y);
			// context.write(new Text(), new
			// Text(Bytes.toString(qualifiername)+"\t"+x+"\t"+y));

		}
	}
}