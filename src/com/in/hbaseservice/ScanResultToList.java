package com.in.hbaseservice;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/4/17.
 */
public class ScanResultToList {

       public List ResultToPhotoList1(ResultScanner rs){
           byte[] key;
           String skey;
           String[] lkey;
           byte[] cellvalue;
           String stringvalue;
           String[] stringvaluelist;
           String x;
           String y;
           String isworkday;
           String qualifiernames;
           ArrayList<Long>  list = new ArrayList<Long>();
         //  List<Object[][]> list=new ArrayList<Object[][]>();
           for (Result result:rs){
               key=result.getRow();
               skey=Bytes.toString(key);
               lkey=skey.split("_");
               list.add(Long.parseLong(lkey[2]));
           }
         return list;
       }


       public List ResultToPhotoList2(ResultScanner rs) {
           byte[] key;
           String skey;
           String[] lkey;
           byte[] cellvalue;
           String stringvalue;
           String[] stringvaluelist;
           String x;
           String y;
           String isworkday;
           String qualifiernames;
           // List<[Long][Long]> list=new ArrayList<[Long][Long]>();
           List<TwoTuple> list = new ArrayList<TwoTuple>();

           for (Result result : rs) {
               key = result.getRow();
               skey = Bytes.toString(key);
               lkey = skey.split("_");
               while (result.advance()) {
                   Cell cell = result.current();
                   cellvalue = cell.getValue();
                   // cell.getTimestamp();
                   stringvalue = Bytes.toString(cellvalue);
                   list.add(new TwoTuple(Long.parseLong(lkey[2]), Long.parseLong(stringvalue)));
               }
           }
           return list;

       }

}
