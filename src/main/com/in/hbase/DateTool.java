package main.com.in.hbase;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTool {

	/**
	 * author:tzl
	 */

	public int getDaysBetween(Calendar d1, Calendar d2) {
		if (d1.after(d2)) {
			Calendar swap = d1;
			d1 = d2;
			d2 = swap;
		}
		int days = d2.get(Calendar.DAY_OF_YEAR) - d1.get(Calendar.DAY_OF_YEAR);
		int y2 = d2.get(Calendar.YEAR);
		if (d1.get(Calendar.YEAR) != y2) {
			d1 = (Calendar) d1.clone();
			do {
				days += d1.getActualMaximum(Calendar.DAY_OF_YEAR);
				d1.add(Calendar.YEAR, 1);
			} while (d1.get(Calendar.YEAR) != y2);
		}
		return days;
	}

	public static int getIntervalDays(Date startday, Date endday) {
		if (startday.after(endday)) {
			Date cal = startday;
			startday = endday;
			endday = cal;
		}
		long sl = startday.getTime();
		long el = endday.getTime();
		long ei = el - sl;
		return (int) (ei / (1000 * 60 * 60 * 24));
	}

	public static int getIntervalDays(String startday, String endday)
			throws ParseException {

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date ddd1 = df.parse(startday);
		Date ddd2 = df.parse(endday);

		if (ddd1.after(ddd2)) {
			Date cal = ddd1;
			ddd1 = ddd2;
			ddd2 = cal;
		}

		long sl = ddd1.getTime();
		long el = ddd2.getTime();
		long ei = el - sl;

		return (int) (ei / (1000 * 60 * 60 * 24));
	}

/*	public static void main(String[] args) throws ParseException, IOException {
		
		// TODO Auto-generated method stub
		String s1="1412836575";
		System.out.println(TimeStamp2Date(s1, "yyyy-MM-dd"));
		System.out.println(getIntervalDays(TimeStamp2Date(URLReader.getGelTime("41234002308"), "yyyy-MM-dd"),"2014-10-12"));

	}*/
	
	public static String TimeStamp2Date(String timestampString, String formats){
		  Long timestamp = Long.parseLong(timestampString)*1000;
		  String date = new SimpleDateFormat(formats).format(new Date(timestamp));
		  return date;
		}

	// 将时间戳转为字符串
	public static String getStrTime(String cc_time) {
		String re_StrTime = null;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
//		long lcc_time = Long.valueOf(cc_time.substring(0, 10));
//		re_StrTime = sdf.format(new Date(lcc_time * 1000L));
		
		re_StrTime=sdf.format(new Date(Long.valueOf(cc_time)));

		return re_StrTime;

	}

	public static String getDate(long date, int format) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date);
		if (format == 2) {
			return String.format("%1$tF:%1$tT", cal);
		} else {
			return String.format("%tF", cal);
		}
	}
	
	public static boolean isWorkDay(String bDate) throws ParseException{
		DateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		Date bdate = format1.parse(bDate);
		Calendar cal = Calendar.getInstance();
		cal.setTime(bdate);
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
				|| cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			return false;
		} else {
			return true;
		}
	}

}
