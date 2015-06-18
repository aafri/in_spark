package com.in.hbaseservice

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by Administrator on 2015/4/28.
 */
class MyHTable(val conf:Configuration ,val  tablename:Array[Byte]) extends  org.apache.hadoop.hbase.client.HTable with Serializable{

}
