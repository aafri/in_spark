package com.in.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间处理相关工具类，提供各种统计过程中所需要的时间格式
 * 
 */
public class LogTimeUtil {

	public static String getDayTime(String str) throws ParseException {
		if(str==null){
			return "19700101";
		}
		String ts="";
		String regex = ("^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})$");
		Pattern pp = Pattern.compile(regex);
		Matcher mm = pp.matcher(str);
		if (mm.find()) {
			SimpleDateFormat sf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.US);
			Date d = sf.parse(str);
			ts= d.getTime() / 1000+"";
		} else {
			ts= "0";
		}
		String timestamp=ts+"000";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(timestamp)));
		return sd;
	}
	public static String getHourTime(String str) throws ParseException {
		if(str==null){
			return "1970010108";
		}
		String ts="";
		String regex = ("^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})$");
		Pattern pp = Pattern.compile(regex);
		Matcher mm = pp.matcher(str);
		if (mm.find()) {
			SimpleDateFormat sf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.US);
			Date d = sf.parse(str);
			ts= d.getTime() / 1000+"";
		} else {
			ts= "0";
		}
		String timestamp=ts+"000";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String sd = sdf.format(new Date(Long.parseLong(timestamp)));
		return sd;
	}
	public static String getMinTime(String str) throws ParseException {
		if(str==null){
			return "197001010800";
		}
		String ts="";
		String regex = ("^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})$");
		Pattern pp = Pattern.compile(regex);
		Matcher mm = pp.matcher(str);
		if (mm.find()) {
			SimpleDateFormat sf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.US);
			Date d = sf.parse(str);
			ts= d.getTime() / 1000+"";
		} else {
			ts= "0";
		}
		String timestamp=ts+"000";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		String sd = sdf.format(new Date(Long.parseLong(timestamp)));
		return sd;
	}
	public static long getTimeStamp(String str) throws ParseException {
		String regex = ("^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})$");
		Pattern pp = Pattern.compile(regex);
		Matcher mm = pp.matcher(str);
		if (mm.find()) {
			SimpleDateFormat sf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.US);
			Date d = sf.parse(str);
			return d.getTime() / 1000;
		} else {
			return 0L;
		}
	}

	public static void main(String[] args) throws IOException, ParseException {
		Map<String, List<String>> groupResult = new HashMap<String, List<String>>();

		// p1 u1 1
		// p1 u2 5
		// p1 u3 3
		// p2 u1 1
		// p2 u2 3
		String t="01/Feb/2015:00:00:02";
		try {
			System.out.println(getTimeStamp(t));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*FileReader fr = new FileReader("f://itugoxxx.txt");
		BufferedReader bufr = new BufferedReader(fr);

		String line = null;
		String[] strs = null;

		List<String> list = new ArrayList<String>();
		String lastTarget = null;

		while ((line = bufr.readLine()) != null) {

			System.out.println(line);
			strs = line.split("\t");

			if (lastTarget == null) {
				lastTarget = strs[0];
			}
			String target = strs[0];

			// add up the count of target
			if (target.equals(lastTarget)) {
				System.out.println("in");
				list.add(strs[1]);
				// System.out.println("list+1"+strs[1]);
				lastTarget = target;

			} else {
				System.out.println("in1:" + lastTarget);
				System.out.println(list);
				groupResult.put(lastTarget, list);
				list = new ArrayList<String>();
				list.add(strs[1]);
				lastTarget = target;

			}

		}

		bufr.readLine();
		bufr.close();*/

		// Map<String, List<String>> groupResult = new HashMap<String,
		// List<String>>();
		// List<String> list1 = new ArrayList<String>();
		// List<String> list2 = new ArrayList<String>();
		// List<String> list3 = new ArrayList<String>();
		// list1.add("uk1");
		// list1.add("uk2");
		// list1.add("uk3");
		// groupResult.put("pid1", list1);
		// list1.clear();
		// list1.add("uk2");
		// list1.add("uk3");
		// groupResult.put("pid2", list2);
		// list1.clear();
		// list1.add("uk1");
		// list1.add("uk3");
		// groupResult.put("pid3", list3);

		System.out.println("begin");

		try {
			String s=null;
			System.out.println(getMinTime(s));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(getMinTime("18/Mar/2015:01:00:01"));
		// Iterator iterator = groupResult.keySet().iterator();
		// while (iterator.hasNext()) {
		//
		// String key = iterator.next().toString();
		// System.out.println(key);
		//
		// List<String> value = groupResult.get(key);
		// for (int i = 0; i < value.size(); i++) {
		// System.out.println(value.get(i));
		// }
		// }


	}
	
	  
}
