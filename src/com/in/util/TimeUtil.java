package com.in.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间处理相关工具类，提供各种统计过程中所需要的时间格式
 * 
 */
public class TimeUtil {
	
	private static final int INTERVAL_HOUR = 3600;// 每小时秒数
	private static final int INTERVAL_DAY = 86400;// 每天秒数
	
	public static final int DATE_SHORT_FORMAT = 1;
	public static final int DATE_LONG_FORMAT = 2;
	
	public static Map<String,String> MonInUsStyle = new HashMap<String,String>();
	static {
		MonInUsStyle.put("Jan", "01");
		MonInUsStyle.put("Feb", "02");
		MonInUsStyle.put("Mar", "03");
		MonInUsStyle.put("Apr", "04");
		MonInUsStyle.put("May", "05");
		MonInUsStyle.put("Jun", "06");
		MonInUsStyle.put("Jul", "07");
		MonInUsStyle.put("Aug", "08");
		MonInUsStyle.put("Sep", "09");
		MonInUsStyle.put("Oct", "10");
		MonInUsStyle.put("Nov", "11");
		MonInUsStyle.put("Dec", "12");
	}
	
	public static Map<String,String> MonInNumStyle = new HashMap<String,String>();
	static {
		MonInNumStyle.put("01", "Jan");
		MonInNumStyle.put("02", "Feb");
		MonInNumStyle.put("03", "Mar");
		MonInNumStyle.put("04", "Apr");
		MonInNumStyle.put("05", "May");
		MonInNumStyle.put("06", "Jun");
		MonInNumStyle.put("07", "Jul");
		MonInNumStyle.put("08", "Aug");
		MonInNumStyle.put("09", "Sep");
		MonInNumStyle.put("10", "Oct");
		MonInNumStyle.put("11", "Nov");
		MonInNumStyle.put("12", "Dec");
	}
	
	
	private static Calendar cal = Calendar.getInstance();
	
	public static final SimpleDateFormat formatDate = new SimpleDateFormat(
	"yyyy-MM-dd");
	public static final SimpleDateFormat userpformatDate = new SimpleDateFormat(
			"yyyyMMdd");
	
	/**
	 * 根据日期字符值来增加或者减少相应的天数。
	 * @param date String. 日期字面值
	 * @param interval int. 相对于当前的天数
	 * @return
	 */

	/**
	 * 根据日期字符值来增加或者减少相应的天数。
	 * @param date String. 日期字面值
	 * @param interval int. 相对于当前的天数
	 * @return
	 */
	public static String addDate(String date,int interval){
		try {
			cal.setTimeInMillis(formatDate.parse(date).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.add(Calendar.DATE, interval);
		return String.format("%tF", cal);
	}
	
	/**
	 * 根据毫秒数来获取相应的时间字面值。
	 * @param date long. 毫秒数。
	 * @param format int. 日期或者时间戳类型。
	 * @return
	 */
	public static String getDate(long date,int format) {
		cal.setTimeInMillis(date);	
		if(format == TimeUtil.DATE_LONG_FORMAT){
			return String.format("%1$tF:%1$tT", cal);
		}else{
			return String.format("%tF", cal);
		}
	}
	
	/**
	 * 将输入的时间字符串与时间戳正则表达式匹配，若能匹配则返回对应日期的毫秒值
	 */
	public static long getDateInLongFormat(String date){
		Calendar cal = Calendar.getInstance();
		String regex = "^(\\d{4})-(\\d{2})-(\\d{2})(?:[ :_-](\\d{2})[:_-](\\d{2})[:_-](\\d{2}))?$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(date);
		if (matcher.matches()){
			cal.set(Calendar.YEAR,Integer.parseInt(matcher.group(1)));
			cal.set(Calendar.MONTH,Integer.parseInt(matcher.group(2))-1);
			cal.set(Calendar.DATE,Integer.parseInt(matcher.group(3)));
			if(matcher.group(4) != null){
				cal.set(Calendar.HOUR_OF_DAY,Integer.parseInt(matcher.group(4)));
				cal.set(Calendar.MINUTE,Integer.parseInt(matcher.group(5)));
				cal.set(Calendar.SECOND,Integer.parseInt(matcher.group(6)));
			}
			else{
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
			}
			cal.set(Calendar.MILLISECOND, 0);
		}
		return cal.getTimeInMillis();
	}
	
	/**
	 * 将带毫秒的日期字符串转换成long
	 * @param date
	 * @return
	 */
	public static long getDateInLongInMillis(String date){
		String[] secs = date.split(",");
		if(secs.length == 2){
			return getDateInLongFormat(secs[0])+Long.parseLong(secs[1]);
		}else{
			return getDateInLongFormat(secs[0]);
		}
	}
	
	/**
	 * 获得相对于当前日期之前的某天的美式日期,如01/Jan/2010
	 * 
	 * 从config配置文件中读取back、offset及interval_seconds等属性值，根据interval_seconds的值来判断
	 * 是否为整天统计还是整点统计，以此来计算相对于当前时间的时间。
	 * @return 日期字符串
	 */
	
	public static String getDateInUSLocale() {
		Properties config = HDFSUtil.getConfig();
		long begin 	= 0L;
		//long end 	= 0L;
		Calendar cal2 = Calendar.getInstance();
		cal2.set(Calendar.MILLISECOND, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MINUTE, 0);
		int back = Integer.parseInt(config.getProperty("back"));
		int offset = Integer.parseInt(config.getProperty("offset"));
		int intervals = Integer.parseInt(config.getProperty("interval_seconds"));
		if (intervals == INTERVAL_HOUR) {
			SimpleDateFormat sf = new SimpleDateFormat(
					"dd/MMM/yyyy:HH",Locale.US);
			//cal.add(Calendar.HOUR_OF_DAY, -1 * back);// 回溯至某小时
			begin = cal2.getTimeInMillis() - INTERVAL_HOUR * offset * 1000L * back;
			//cal.setTimeInMillis(begin);
			return sf.format(begin);
		} else if (intervals == INTERVAL_DAY) {
			SimpleDateFormat sf = new SimpleDateFormat(
			"dd/MMM/yyyy",Locale.US);
			cal2.set(Calendar.HOUR_OF_DAY, 0);
			//cal.add(Calendar.DATE, -1 * back);// 回溯至某天
			begin = cal2.getTimeInMillis() - INTERVAL_DAY * offset * 1000L * back;
			//cal.setTimeInMillis(begin);
			return sf.format(begin);
		}else {
			return null;
		}
	}

	public static long getDiffStringDate(Date dt, int diff) {
		Calendar ca = Calendar.getInstance();
		if (dt == null) {
			ca.setTime(new Date());
		} else {
			ca.setTime(dt);
		}
		ca.add(Calendar.DATE, diff);
		return ca.getTimeInMillis();
	}
	public static String getLongToLongDate(Long d) {
		long sd=d;
		Date dat=new Date(sd);
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(dat);
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		String sb=format.format(gc.getTime());
		return sb;
	}
	public static String getLongDayDate(String d) {
		String l="";
		SimpleDateFormat  format = new SimpleDateFormat("yyyyMMdd");
		Date date = dateFormat(d);
		l=format.format(date);
		return l;
	}
	/**
	 * 此方法从对应的配置文件中读取相应的时间设置参数，如back、offset及interval_seconds来动态生成对应的日期。<br>
	 * 如back=1，<br>
	 * offset=1，<br>
	 * interval_seconds=86400，<br>
	 * 则在当前2010-07-12运行该程序则返回2010-07-11.
	 * @return 字符日期形式yyyy-mm-dd
	 */
	public static String getDate() {// get back day num for config
		Properties config = HDFSUtil.getConfig();
		long begin 	= 0L;
		//long end 	= 0L;
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		int back = Integer.parseInt(config.getProperty("back"));
		int offset = Integer.parseInt(config.getProperty("offset"));
		int intervals = Integer.parseInt(config.getProperty("interval_seconds"));
		if (intervals == INTERVAL_HOUR) {
			//cal.add(Calendar.HOUR_OF_DAY, -1 * back);// 回溯至某小时
			begin = cal.getTimeInMillis() - INTERVAL_HOUR * offset * 1000L * back;
			//end = begin + INTERVAL_HOUR * offset * 1000L;
		} else if (intervals == INTERVAL_DAY) {
			cal.set(Calendar.HOUR_OF_DAY, 0);
			//cal.add(Calendar.DATE, -1 * back);// 回溯至某�?
			begin = cal.getTimeInMillis() - INTERVAL_DAY * offset * 1000L * back;
			//end = begin + INTERVAL_DAY * offset * 1000L;
		}
		return new java.sql.Date(begin).toString();
	}
	public static Date getNextDay(String d) {
		SimpleDateFormat  format = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = null;
		try {
			date = format.parse(d);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, 1);
		date = calendar.getTime();
		return date;
	}


	/***
	 * 把YYYY-mm-dd格式的日期，转化为yyyyMMdd格式的日期
	 * @param d
	 * @return
	 */
	public static String getLongDate(String d) {
		String l="";
		SimpleDateFormat  format = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = dateFormat(d);
		l=format.format(date);
		return l;
	}

	private static Date dateFormat(String format) {
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-d H:m:s").parse(format);
		} catch (ParseException e) {
			System.out.println("日期不符合规范或日期输入格式错误。");
			System.out.println("请按照\"****(年)-**(月)-**(日) **(小时):**(分钟):**(秒钟)\"的格式输入。");
		}
		return date;
	}
	public static String addDateWithoutMLine(String date,int interval){
		try {
			cal.setTimeInMillis(userpformatDate.parse(date).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.add(Calendar.DATE, interval);
		//Date dates= cal.getTime();
	//	String dates1= cal.getTime().toString();
		String dates= String.format("%tF", cal);

		//System.out.println(dates);
		String pattern = "yyy-MM-d"; //首先定义时间格式
		SimpleDateFormat format = new SimpleDateFormat(pattern);//然后创建一个日期格式化类
		//String toConvertString = "2012-11-01 10:10:05";
		Date convertResult = null;
		try {
			convertResult = format.parse(dates);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String startTime = sdf.format(convertResult);
		return startTime;
	}
	/*public static int daysBetween(Date smdate,Date bdate) throws ParseException
	{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		smdate=sdf.parse(sdf.format(smdate));
		bdate=sdf.parse(sdf.format(bdate));
		Calendar cal = Calendar.getInstance();
		cal.setTime(smdate);
		long time1 = cal.getTimeInMillis();
		cal.setTime(bdate);
		long time2 = cal.getTimeInMillis();
		long between_days=(time2-time1)/(1000*3600*24);

		return Integer.parseInt(String.valueOf(between_days));
	}*/

	/**
	 *字符串的日期格式的计算
	 */
	public static int daysBetween(String smdate,String bdate) throws ParseException{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(smdate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf.parse(bdate));
		long time2 = cal.getTimeInMillis();
		long between_days=(time2-time1)/(1000*3600*24);

		return Integer.parseInt(String.valueOf(between_days));
	}




	public static void main(String[] args){
		//28/Sep/2014:00:06:56
		String predate=TimeUtil.addDateWithoutMLine("20150401",-1);
System.out.print(predate);

		/*SimpleDateFormat sf = new SimpleDateFormat(
				"dd/MMM/yyyy:HH:mm:ss",Locale.US);
		SimpleDateFormat sf1 = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss",Locale.US);
		try {
			Date d1=sf1.parse("2015-01-20 20:42:06");
			//Date d=sf.parse("28/Sep/2014:00:06:56");
			//System.out.println(d.getTime()/1000);
			System.out.println(d1.getTime()/1000);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(getDateInLongFormat("28/Sep/2014:00:06:56"));
		
		Pattern p=Pattern.compile("\\d+.\\d+");
		Matcher m=p.matcher("0.321");
		System.out.println(m.matches());*/
	}
	
	  
}
