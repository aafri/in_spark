package main.com.in.hbase;

import com.in.hbase.RegionSplitsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;

public class CreateRegion {
	
	 private static Log logger = LogFactory.getLog(CreateRegion.class);
	 public void run(String[] args) throws Exception {
		  
		    String tableName = args[0];
		    String columnName = args[1];
		    int regionNum = args[2] == null ? 10 : Integer.parseInt(args[2]);
		    HBaseAdmin admin = null;
		    try {
		      Configuration conf = HBaseConfiguration.create();
		      admin = new HBaseAdmin(conf);
		      if (admin.tableExists(tableName)) {
		        logger.warn("table : " + tableName + " already exist.");
		        return;
		      }

		      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		      HColumnDescriptor columnDesc = new HColumnDescriptor(columnName);
		      tableDesc.addFamily(columnDesc);
		      admin.createTable(tableDesc, RegionSplitsUtil.splits(regionNum));
		      admin.getTableDescriptor(tableName.getBytes()).toString();

		      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		      //writeTableConf(conf, otherArgs);

		      if (logger.isInfoEnabled())
		        logger.info("table : " + tableName + " created sucessfully.");
		    }
		    catch (Exception e) {
		      throw new Exception("failed create region", e);
		    }
		  }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
