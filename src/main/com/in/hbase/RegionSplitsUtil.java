package main.com.in.hbase;
import java.math.BigInteger;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
public class RegionSplitsUtil {

	
	
	 static final String MAXMD5 = "FFF";
	  static final int rowComparisonLength = "FFF".length();

	  
		/*public byte[][] getSplitRowKeys() {
			Object splitRowKeyList;
			if (CollectionUtil.isEmpty(splitRowKeyList)) {
				return new byte[0][0];
			}
			byte[][] splits = new byte[((Object) splitRowKeyList).size()][];
			int length = splits.length;
			for (int i = 0; i < length; ++i) {
				splits[i] = Bytes.toBytes(splitRowKeyList.get(i));
			}
			return splits;
		}*/
	  static byte[] convertToByte(BigInteger bigInteger) { String bigIntegerString = bigInteger.toString(16);
	    bigIntegerString = StringUtils.leftPad(bigIntegerString, rowComparisonLength, '0');
	    return Bytes.toBytes(bigIntegerString); }

	  public static byte[][] splits(String startKey, String endKey, int numRegions) {
	    byte[][] splits = new byte[numRegions][];
	    BigInteger lowestKey = new BigInteger(startKey, 16);
	    BigInteger highestKey = new BigInteger(endKey, 16);
	    BigInteger range = highestKey.subtract(lowestKey);
	    BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
	    lowestKey = lowestKey.add(regionIncrement);
	    for (int i = 0; i < numRegions; i++) {
	      BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
	      //System.out.println(key);
	      byte[] b = convertToByte(key);
	      System.out.println(b);
	      splits[i] = b;
	    }
	    return splits;
	  }
	  /**表名称**/
	  
	  private String tableName;

		/**
		 * 列族集合
		 */

		/**
		 * 最大的region的size(一个列族的存储单元的大小)
		 */
		private long maxFileSize;

		/**
		 * memStore的flush到HDFS上的文件大小
		 */
		private long memstoreFlushSize;

		/**
		 * 开始的主键
		 */
		private String startRowKey;

		/**
		 * 结束的主键
		 */
		private String endRowKey;

		/**
		 * 预分region的数量
		 */
		private int regionNumber;

		/**
		 * 预分region的主键集合
		 */
		private static List<String> splitRowKeyList=Lists.newArrayList();;

		/**
		 * 表是否只读(默认为false)
		 */
		private boolean isReadOnly = false;

		/**
		 * 是否延时日志刷写(默认为false)
		 */
		private boolean isDeferredLogFlush = false;

		public static  void addSplitRowKey(String rowKey) {
			
			splitRowKeyList.add(rowKey);
		}
 
		public static byte[][] getIPSplitRowKeys() {
		    System.out.println("region number:"+splitRowKeyList.size()*splitRowKeyList.size());
		    int s=splitRowKeyList.size();
			byte[][] splits = new byte[s][];
			int length = splits.length;
			
		   // System.out.println("splits length:"+splitRowKeyList.size()*splitRowKeyList.size());
            int d=0;
			for (int i = 0; i < s; i++) {						
					splits[d] = Bytes.toBytes(splitRowKeyList.get(i));;
					d++;
					System.out.println("region number:" +d);
				}
				
			
			return splits;
		}
		public static byte[][] getLoghouseSplitRowKeys() {
		    System.out.println("region number:"+splitRowKeyList.size()*splitRowKeyList.size());
		    int s=splitRowKeyList.size()*splitRowKeyList.size();
		    int n=splitRowKeyList.size();
			//byte[][] splits = new byte[686][];
		    byte[][] splits = new byte[s][];
			int length = splits.length;
			//Pattern p = Pattern.compile("[0-9]");
			//Matcher m=null;
		    System.out.println("splits length:"+splitRowKeyList.size()*splitRowKeyList.size());
            int d=0;
			for (int i = 0; i < n; ++i) {	
				/*System.out.println("N:" +n);
                if(p.matcher(splitRowKeyList.get(i)).matches()){
    				System.out.println("match number:" +splitRowKeyList.get(i));

                	splits[d]=Bytes.toBytes(splitRowKeyList.get(i));
                	d++;
                	break;}*/ 
				for (int j = 0; j < n; ++j) {	
					/*if(p.matcher(splitRowKeyList.get(j)).matches()){
						break;
					}*/
					splits[d] = Bytes.toBytes(splitRowKeyList.get(i)+""+splitRowKeyList.get(j));;
					d++;
					System.out.println("region number:" +d);
				}	
			}
			System.out.println("end get region number:" +d);
			return splits;
		}
		public static byte[][] getSplitRowKeys() {
		    System.out.println("region number:"+splitRowKeyList.size()*splitRowKeyList.size());
		    int s=splitRowKeyList.size()*splitRowKeyList.size();
		    int n=splitRowKeyList.size();
			byte[][] splits = new byte[s][];
			int length = splits.length;		
		    System.out.println("splits length:"+splitRowKeyList.size()*splitRowKeyList.size());
            int d=0;
			for (int i = 0; i < n; ++i) {		
				for (int j = 0; j < n; ++j) {		
                     
					splits[d] = Bytes.toBytes(splitRowKeyList.get(i)+""+splitRowKeyList.get(j));;
					d++;
					System.out.println("region number:" +d);
				}
				
			}
			return splits;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}


		public long getMaxFileSize() {
			return maxFileSize;
		}

		public void setMaxFileSize(long maxFileSize) {
			this.maxFileSize = maxFileSize;
		}

		public long getMemstoreFlushSize() {
			return memstoreFlushSize;
		}

		public void setMemstoreFlushSize(long memstoreFlushSize) {
			this.memstoreFlushSize = memstoreFlushSize;
		}

		public String getStartRowKey() {
			return startRowKey;
		}

		public void setStartRowKey(String startRowKey) {
			this.startRowKey = startRowKey;
		}

		public String getEndRowKey() {
			return endRowKey;
		}

		public void setEndRowKey(String endRowKey) {
			this.endRowKey = endRowKey;
		}

		public int getRegionNumber() {
			return regionNumber;
		}

		public void setRegionNumber(int regionNumber) {
			this.regionNumber = regionNumber;
		}

		public boolean isReadOnly() {
			return isReadOnly;
		}

		public void setReadOnly(boolean isReadOnly) {
			this.isReadOnly = isReadOnly;
		}

		public boolean isDeferredLogFlush() {
			return isDeferredLogFlush;
		}

		public void setDeferredLogFlush(boolean isDeferredLogFlush) {
			this.isDeferredLogFlush = isDeferredLogFlush;
		}

		public List<String> getSplitRowKeyList() {
			return splitRowKeyList;
		}

		public void setSplitRowKeyList(List<String> splitRowKeyList) {
			this.splitRowKeyList = splitRowKeyList;
		}

		@Override
		public String toString() {
			return "HBaseTableDescribesEntity [tableName=" + tableName+ ", maxFileSize=" + maxFileSize
					+ ", memstoreFlushSize=" + memstoreFlushSize + ", startRowKey="
					+ startRowKey + ", endRowKey=" + endRowKey + ", regionNumber="
					+ regionNumber + ", splitRowKeyList=" + splitRowKeyList
					+ ", isReadOnly=" + isReadOnly + ", isDeferredLogFlush="
					+ isDeferredLogFlush + "]";
		}
	  
	  public static byte[][] splits(int numRegions) {
		    int total = 0;
		    int length = 0;
		    if (numRegions < 3) {
		      throw new UnsupportedOperationException("the number of ranges must more than 3");
		    }
		    if (numRegions < 16) {
		      total = 256;
		      length = 2;
		    }
		    else if (numRegions < 256) {
		      total = 4096;
		      length = 3;
		    }
		    else if (numRegions < 4096) {
		      total = 65536;
		      length = 4;
		    }
		    else {
		      throw new UnsupportedOperationException("not support more than 16 * 16 * 16 of ranges");
		    }
		    byte[][] result = new byte[numRegions][length];
		    float div = total / (numRegions - 1);
		    for (int i = 0; i < numRegions - 1; i++) {
		      int curr = (int)(i * div);
		      byte[] temp = Integer.toHexString(curr).getBytes();
		      if (temp.length < length) {
		        for (int j = 0; j < length - temp.length; j++) {
		          result[i][j] = 48;
		          for (int k = 0; k < temp.length; k++) {
		          result[i][j] = temp[k];
		         j++;
		         
		        }
		      }
		      } else {
		        result[i] = temp;
		      }
		    }
		    for (int i = 0; i < length; i++) {
		      result[(numRegions - 1)][i] = 102;
		    }

		    return result;
		  }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String startKey="256";
		  BigInteger lowestKey = new BigInteger(startKey, 16);
		  byte[] b = convertToByte(lowestKey);
		  System.out.println(b);
	}

}
