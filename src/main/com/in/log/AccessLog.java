package main.com.in.log;

import com.in.config.MatchUnit;
import com.in.log.*;
import com.in.logtrasfer.TrasferedValue;
import com.in.util.Consnt;
import com.in.util.TimeUtil;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AccessLog {

    /**
     * 解析一条日志，返回相应的结果 1、去除日志记录中的引号 2、 如果字段长度(以空格分隔)小于15即判断该条记录非法 3、
     * 如果日志中的时间戳非不在统计当日的时间区间内，也不计入统计
     *
     * @param line    一条Log记录
     * @param isParma 如果为true，则保留request部分？后的参数，否则去除。
     * @return 返回字段名和相应的字段值的组成的Map集合
     */
    public static Map<String, String> parseLog(String line, boolean isParma) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            // 获取当前的日志的时间戳
            String date = TimeUtil.getDateInUSLocale();
            String[] item = line.split("\\s");
            int itemLength = item.length;

            /**
             * 如果字段数小于15，或者第3个字段不是以对应的日期开头，则返回空
             */
            if (itemLength < 15 || !item[3].startsWith("[" + date)) {
                return null;
            }

            String ip = item[0];
            String req = item[5] + " " + item[6] + " " + item[7];
            String referer = item[10];
            String hostName = item[12];
            String cookie = item[13];
            String httpstate = item[8];

            // 对request的参数部分进行处理
            if (!isParma) {
                int idxP = req.indexOf("?");
                if (idxP != -1) {
                    req = req.substring(0, idxP);
                }
            }
            map.put(Consnt.LOGITEM_IP, ip);
            map.put(Consnt.LOGITEM_REFERER, referer);
            map.put(Consnt.LOGITEM_COOKIE, cookie);
            map.put(Consnt.LOGITEM_HOSTNAME, hostName);
            map.put(Consnt.LOGITEM_REQ, req);
            map.put(Consnt.LOGITEM_ALL, line);
            map.put(Consnt.LOGITEM_STATE, httpstate);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static List<String> parseLog(String input, Pattern pattern) {
        List<String> fieldList = new ArrayList<String>();
        Matcher m = pattern.matcher(input);
        if (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                fieldList.add(m.group(i));
            }
        }
        return fieldList;
    }

    /**
     * 根据不同的日志类型来选择正则匹配类型，若有新增的日志类型，需要修改该类
     */
    public static Map<String, String> parseLog2Map(String input, String logType) {

        String sarray[] = input.toString().split("\\[");
        String str = input.substring(sarray[0].length() + 1, input.length());

        Map<String, String> fieldMap = new HashMap<String, String>();

        if (logType.equals("ACCESSLOG")) {
            Pattern pattern = com.in.log.LogRegex.ACCESSLOG;
            Matcher m = pattern.matcher(str);
            if (m.find()) {
                fieldMap.put(Consnt.LOGITEM_TIMESTAMP, m.group(1));
                fieldMap.put(Consnt.LOGITEM_REQ, m.group(2));
                fieldMap.put(Consnt.LOGITEM_STATE, m.group(3));
                fieldMap.put(Consnt.LOGITEM_SIZE, m.group(4));
                fieldMap.put(Consnt.LOGITEM_REFERER, m.group(5));
                fieldMap.put(Consnt.LOGITEM_HOSTNAME, m.group(7));
                fieldMap.put(Consnt.LOGITEM_AK, m.group(13));
                //fieldMap.put(Consnt.LOGITEM_RESPONSE, m.group(14));
            }
        }

        return fieldMap;
    }

    public static Map<String, String> parseClog2Map(String input, String logType) {
        Map<String, String> fieldMap = new HashMap<String, String>();

        if (logType.equals("ACCESSLOG")) {
            Pattern pattern = com.in.log.LogRegex.ACCESSLOGTOTAL;
            Matcher m = pattern.matcher(input);
            if (m.find()) {
                fieldMap.put(Consnt.LOGITEM_IP, m.group(1));
                fieldMap.put(Consnt.LOGITEM_TIMESTAMP, m.group(2));
                fieldMap.put(Consnt.LOGITEM_REQ, m.group(3));
                fieldMap.put(Consnt.LOGITEM_STATE, m.group(4));
                fieldMap.put(Consnt.LOGITEM_SIZE, m.group(5));
                fieldMap.put(Consnt.LOGITEM_REFERER, m.group(6));
                fieldMap.put(Consnt.LOGITEM_HOSTNAME, m.group(8));
                fieldMap.put(Consnt.LOGITEM_AK, m.group(13));
                fieldMap.put(Consnt.LOGITEM_RESPONSE, m.group(14));
            }
        }

        return fieldMap;
    }

    public static TrasferedValue genTraferedValue(Map<String, String> trasferMap, Text value) {

        TrasferedValue trasferedValue = new TrasferedValue();
        return trasferedValue;
    }
    /**
     * 各个Match之间是OR的关系，故只要有1个Match符合过滤
     *
     * @param
     * @return 这里返回的boolean 只是为了统计有多少日志行被匹配
     */
    public static boolean filterGroup(List<MatchUnit> filter,
                                      Map<String, String> fields) {
        boolean flag = false;
        for (MatchUnit a : filter) {
            flag = flag || a.matches(fields);
        }
        return flag;
    }


}
