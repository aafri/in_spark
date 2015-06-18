package main.com.in.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.in.hbase.RegionSplitsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseService {
    private static Configuration conf = HBaseConfiguration.create();
	 
	private static HTable connectHBase(String tablename) 
	throws IOException {
	HTable table = null;
	table = new HTable(conf, tablename);
	return table;
	}
	public static  void createTable(String tableName, String[] columnFamilys)  
            throws Exception {  
        // 新建一个数据库管理员  
        HBaseAdmin hAdmin = new HBaseAdmin(conf);  
  
        if (hAdmin.tableExists(tableName)) {  
            System.out.println("表已经存在");  
            System.exit(0);  
        } else {  
            // 新建一个 scores 表的描述  
            @SuppressWarnings("deprecation")
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
           // HColumnDescriptor
           // ((HColumnDescriptor) tableDesc).setCompressionType("");
            //tableDesc.
		//	HTableDescriptor tableDesc = new  HTableDescriptor(tableName);  
            // 在描述里添加列族  
           // Algorithm al=new Algorithm("gz");
           // HTableDescriptor desc = new HTableDescriptor();
           // desc.setName(hbaseTable.getBytes());
          //  colDesc = new HColumnDescriptor("cf1");
          //  colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
          //  desc.addFamily(colDesc);
            for (String columnFamily : columnFamilys) {  
            	HColumnDescriptor cfDesc=new HColumnDescriptor(columnFamily);
            	cfDesc.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.LZO);
            	//cfDesc.se.setCompressionType();
            	//2592000 ..30 days
            	cfDesc.setTimeToLive(2592000);
                tableDesc.addFamily(cfDesc);  
            }  
          //  HColumnDescriptor cfDesc=tableDesc.getFamily(column)
            RegionSplitsUtil.addSplitRowKey("0");
            RegionSplitsUtil.addSplitRowKey("1");
            RegionSplitsUtil.addSplitRowKey("2");
            RegionSplitsUtil.addSplitRowKey("3");
            RegionSplitsUtil.addSplitRowKey("4");
            RegionSplitsUtil.addSplitRowKey("5");
            RegionSplitsUtil.addSplitRowKey("6");
            RegionSplitsUtil.addSplitRowKey("7");
            RegionSplitsUtil.addSplitRowKey("8");
            RegionSplitsUtil.addSplitRowKey("9");
        /*    RegionSplitsUtil.addSplitRowKey("a");
            RegionSplitsUtil.addSplitRowKey("b");
            RegionSplitsUtil.addSplitRowKey("c");
            RegionSplitsUtil.addSplitRowKey("d");
            RegionSplitsUtil.addSplitRowKey("e");
            RegionSplitsUtil.addSplitRowKey("f");*/
            // 根据配置好的描述建表  
            //hAdmin.createTable(tableDesc);  
            //hAdmin.createTable(arg0, arg1);
           hAdmin.createTable(tableDesc, RegionSplitsUtil.getSplitRowKeys());
            System.out.println("创建表成功");  
        }  
    }  
	
	
	public static  void createLoghouseTable(String tableName, String[] columnFamilys)  throws Exception {  
		HBaseAdmin hAdmin = new HBaseAdmin(conf);  
		  
        if (hAdmin.tableExists(tableName)) {  
            System.out.println("表已经存在");  
            System.exit(0);  
        } else {  
            // 新建一个 scores 表的描述  
            @SuppressWarnings("deprecation")
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
           // HColumnDescriptor
           // ((HColumnDescriptor) tableDesc).setCompressionType("");
            //tableDesc.
		//	HTableDescriptor tableDesc = new  HTableDescriptor(tableName);  
            // 在描述里添加列族  
           // Algorithm al=new Algorithm("gz");
           // HTableDescriptor desc = new HTableDescriptor();
           // desc.setName(hbaseTable.getBytes());
          //  colDesc = new HColumnDescriptor("cf1");
          //  colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
          //  desc.addFamily(colDesc);
            for (String columnFamily : columnFamilys) {  
            	HColumnDescriptor cfDesc=new HColumnDescriptor(columnFamily);
            	cfDesc.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.LZO);
            	//cfDesc.se.setCompressionType();
            	//2592000 ..30 daysgetLoghouseSplitRowKeys
            	cfDesc.setTimeToLive(2598600);
                tableDesc.addFamily(cfDesc);                 
            }  
          //  HColumnDescriptor cfDesc=tableDesc.getFamily(column)
            RegionSplitsUtil.addSplitRowKey("a");
            RegionSplitsUtil.addSplitRowKey("b");
            RegionSplitsUtil.addSplitRowKey("c");
            RegionSplitsUtil.addSplitRowKey("d");
            RegionSplitsUtil.addSplitRowKey("e");
            RegionSplitsUtil.addSplitRowKey("f");
            RegionSplitsUtil.addSplitRowKey("g");
            RegionSplitsUtil.addSplitRowKey("h");
            RegionSplitsUtil.addSplitRowKey("i");
            RegionSplitsUtil.addSplitRowKey("j");
            RegionSplitsUtil.addSplitRowKey("k");
            RegionSplitsUtil.addSplitRowKey("l");
            RegionSplitsUtil.addSplitRowKey("m");
            RegionSplitsUtil.addSplitRowKey("n");
            RegionSplitsUtil.addSplitRowKey("o");
            RegionSplitsUtil.addSplitRowKey("p");
            RegionSplitsUtil.addSplitRowKey("q");
            RegionSplitsUtil.addSplitRowKey("r");
            RegionSplitsUtil.addSplitRowKey("s");
            RegionSplitsUtil.addSplitRowKey("t");
            RegionSplitsUtil.addSplitRowKey("u");
            RegionSplitsUtil.addSplitRowKey("v");
            RegionSplitsUtil.addSplitRowKey("w");
            RegionSplitsUtil.addSplitRowKey("x");
            RegionSplitsUtil.addSplitRowKey("y");
            RegionSplitsUtil.addSplitRowKey("z");
            RegionSplitsUtil.addSplitRowKey("0");
            RegionSplitsUtil.addSplitRowKey("1");
            RegionSplitsUtil.addSplitRowKey("2");
            RegionSplitsUtil.addSplitRowKey("3");
            RegionSplitsUtil.addSplitRowKey("4");
            RegionSplitsUtil.addSplitRowKey("5");
            RegionSplitsUtil.addSplitRowKey("6");
            RegionSplitsUtil.addSplitRowKey("7");
            RegionSplitsUtil.addSplitRowKey("8");
            RegionSplitsUtil.addSplitRowKey("9");
            // 根据配置好的描述建表  
            //hAdmin.createTable(tableDesc);  
            //hAdmin.createTable(arg0, arg1);
           hAdmin.createTable(tableDesc, RegionSplitsUtil.getLoghouseSplitRowKeys());
            System.out.println("创建表成功");  
        }
	
	}
	
	public static  void createIPTable(String tableName, String[] columnFamilys)  
            throws Exception {  
        // 新建一个数据库管理员  
        HBaseAdmin hAdmin = new HBaseAdmin(conf);  
  
        if (hAdmin.tableExists(tableName)) {  
            System.out.println("表已经存在");  
            System.exit(0);  
        } else {  
            // 新建一个 scores 表的描述  
            @SuppressWarnings("deprecation")
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
           // HColumnDescriptor
           // ((HColumnDescriptor) tableDesc).setCompressionType("");
            //tableDesc.
		//	HTableDescriptor tableDesc = new  HTableDescriptor(tableName);  
            // 在描述里添加列族  
           // Algorithm al=new Algorithm("gz");
           // HTableDescriptor desc = new HTableDescriptor();
           // desc.setName(hbaseTable.getBytes());
          //  colDesc = new HColumnDescriptor("cf1");
          //  colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
          //  desc.addFamily(colDesc);
            for (String columnFamily : columnFamilys) {  
            	HColumnDescriptor cfDesc=new HColumnDescriptor(columnFamily);
            	cfDesc.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.LZO);
            	//cfDesc.se.setCompressionType();
            	//2592000 ..30 days
            	cfDesc.setTimeToLive(2592000);
                tableDesc.addFamily(cfDesc);  
                
            }

          //  HColumnDescriptor cfDesc=tableDesc.getFamily(column)
          for(int i=1;i<256;i++){
            RegionSplitsUtil.addSplitRowKey(String.valueOf(i));
        
          }
            // 根据配置好的描述建表  
            //hAdmin.createTable(tableDesc);  
            //hAdmin.createTable(arg0, arg1);
           hAdmin.createTable(tableDesc, RegionSplitsUtil.getIPSplitRowKeys());
            System.out.println("创建表成功");  
        }  
    }  
  
    // 删除数据库表  
    public static  void deleteTable(String tableName) throws Exception {  
        // 新建一个数据库管理员  
        HBaseAdmin hAdmin = new HBaseAdmin(conf);  
  
        if (hAdmin.tableExists(tableName)) {  
            // 关闭一个表  
            hAdmin.disableTable(tableName);  
            // 删除一个表  
            hAdmin.deleteTable(tableName);  
            System.out.println("删除表成功");  
  
        } else {  
            System.out.println("删除的表不存在");   
        }  
    }  
    //
    public static  void truncateTable(TableName tableName) throws Exception {  
        // 新建一个数据库管理员  
        HBaseAdmin hAdmin = new HBaseAdmin(conf);  
  
        if (hAdmin.tableExists(tableName)) {  
            // 关闭一个表  
            hAdmin.disableTable(tableName);  
            // 删除一个表  
            hAdmin.deleteTable(tableName);
            System.out.println("删除表成功");  
           // hAdmin.disableTable(tableName);
        } else {  
            System.out.println("删除的表不存在");  
            System.exit(0);  
        }  
    }  
  
    // 添加一条数据  
    public  void addRow(String tableName, String row,  
            String columnFamily, String column, String value) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Put put = new Put(Bytes.toBytes(row));  
        // 参数出分别：列族、列、值  
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),  
                Bytes.toBytes(value));  
        table.put(put);  
    }  
  
    // 删除一条数据  
    public  void delRow(String tableName, String row) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Delete del = new Delete(Bytes.toBytes(row));  
        table.delete(del);  
    }  
  
    // 删除多条数据  
    public  void delMultiRows(String tableName, String[] rows)  
            throws Exception {  
        HTable table = new HTable(conf, tableName);  
        List<Delete> list = new ArrayList<Delete>();  
  
        for (String row : rows) {  
            Delete del = new Delete(Bytes.toBytes(row));  
            list.add(del);  
        }  
  
        table.delete(list);  
    }  
 
    
    // get row  
    public static  void getRow(String tableName, String row) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Get get = new Get(Bytes.toBytes(row));  
        Result result = table.get(get);  
        // 输出结果  
        
        System.out.println("--"+Bytes.toString(result.listCells().get(0).getFamilyArray()));
        for (KeyValue rowKV : result.raw()) {  
        	 System.out.print("Row Name: " + Bytes.toString(rowKV.getRow()) + " 11 ");  
             System.out.print("Timestamp: " + rowKV.getTimestamp() + " 22 ");  
             System.out.print("column Family: " + Bytes.toString(rowKV.getFamily()) + " 33 ");  
             System.out.print("qualifier:  " + Bytes.toString(rowKV.getQualifier()) + " 44 ");  
             System.out.println("Value: " + Bytes.toString(rowKV.getValue()) + " 55 ");  
        }  
    }  
  
    // get all records  
    public  void getAllRows(String tableName) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Scan scan = new Scan();  
        ResultScanner results = table.getScanner(scan);  
        // 输出结果  
        for (Result result : results) {  
            for (KeyValue rowKV : result.raw()) {  
                System.out.print("Row Name: " + Bytes.toString(rowKV.getKey()) + " ");  
                System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");  
                System.out.print("column Family: " + Bytes.toString(rowKV.getFamilyArray()) + " ");  
                System.out.print("Row Name:  " + Bytes.toString(rowKV.getQualifierArray()) + " ");  
                System.out.println("Value: " + Bytes.toString(rowKV.getValueArray()) + " ");  
            }  
        }  
    }  

/*private  Connection connectDB() 
throws Exception {
String userName = "db_user";
String password = "db_password";
String url = "jdbc:mysql://db_host/database";
Class.forName("com.mysql.jdbc.Driver").newInstance();
Connection conn = (Connection) DriverManager.getConnection(url,
userName, password);
return conn;
}*/
	public static void main(String[] args) throws Exception {
		//HTable gsp=connectHBase("gps");
		 Configuration hbaseconf = null;  
       hbaseconf = HBaseConfiguration.create();  
      // HBaseAdmin hadmin = new HBaseAdmin(hbaseconf);  
      deleteTable("address");
    String[] cf=new String[1];
    cf[0]="f";
    createTable("address", cf);
   // createIPTable("gps_ip_user", cf);
      // byte[][] b=null;
     // byte i=(byte) 0;
       //HbaseService.getRow("gps", "0000a6272407e1fa352e74c751ff9828");      
      // String b1= Integer.toHexString(i);
    //  b= RegionSplitsUtil.splits("0","1703936",24);
      //b= RegionSplitsUtil.splits(24);
      //MessageDigest md = MessageDigest.getInstance("MD5");
      //byte[] digest = md.digest(Bytes.toBytes(s));
      //System.out.println( b1); 
      /* String rowkey="ffffd046bd952ac89ab7d4e662feddb5";
       String columnFamily="f";
       String column="141760483";
       String value="125.416815"+" "+"43.788274";
       Put put = new Put(Bytes.toBytes(rowkey));  
       // 鍙傛暟鍑哄垎鍒細鍒楁棌銆佸垪銆佸�  
     System.out.println("init put value !");
       put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),  
               Bytes.toBytes(value));  
       System.out.println("Start put value to hbase...");
       gsp.put(put);     
       System.out.println("finished put value to hbase:");
       System.out.println(columnFamily+"  "+ column +"  "+value);     
       getRow("gps","ffffd046bd952ac89ab7d4e662feddb5");*/
	    
	}
	

}
