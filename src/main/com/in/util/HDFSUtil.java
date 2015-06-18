package main.com.in.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import com.hadoop.mapreduce.LzoTextInputFormat;


/**
 * HDFS相关的静态类
 * 
 * @author Tang Zeliang
 * 
 */
public class HDFSUtil {

	private static Logger log = Logger.getLogger(HDFSUtil.class);

	// accesslog_20140920-224023431+0800.XXXX.lzo
	private static Pattern ptn = Pattern.compile("(\\d{8}-\\d{9})\\+0800\\.");
	private static Matcher mat = null;
	private static Properties prop = new Properties();

	public static String parseFlumeFileName(String filename) {
		mat = ptn.matcher(filename);
		if (mat.find()) {
			return mat.group(1).substring(0, 15);
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * 从HDFS日志路径中获取对应的文件路径。在相应的时间区间内，向前一区间取最后一个文件，向后一区间取第一个文件。
	 * 
	 * @param parents
	 *            可以用逗号分隔多个目录结构
	 * @param child
	 * @return
	 * @throws java.io.IOException
	 */
	public static String getPaths(String parents, String child)
			throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (String parent : Arrays.asList(parents.split(","))) {

			// 当天的日志数据
			if (fs.exists(new Path(parent, child))) {
				status = fs.listStatus(new Path(parent, child),
						new com.in.util.TmpFileFilter());
				for (FileStatus st : status) {
					set.add(st.getPath().toString());
				}
			}

			// 获取统计日前一天的最后一个压缩文件
			if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, -1)))) {
				status = fs.listStatus(
						new Path(parent, com.in.util.TimeUtil.addDate(child, -1)),
						new com.in.util.TmpFileFilter());
				// 如果前一天存在数据
				if (status != null && status.length > 0) {
					if (status[status.length - 1].getPath().toString()
							.endsWith(".index")) {
						set.add(status[status.length - 2].getPath().toString());
					} else {
						set.add(status[status.length - 1].getPath().toString());
					}
				}
			}
			// 获取统计日后一天的第一个压缩文件
			if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, 1)))) {
				status = fs.listStatus(
						new Path(parent, com.in.util.TimeUtil.addDate(child, 1)),
						new com.in.util.TmpFileFilter());
				if (status != null && status.length > 0) {
					set.add(status[0].getPath().toString());
				}
			}
		}
		cal.add(Calendar.DATE, -1);
		child = String.format("%tF", cal);
		String sb = set.toString();
		return sb.substring(1, sb.length() - 1);
	}

	public static String getPaths(String parent, String child, int type)
			throws IOException {
		Set<String> set = new TreeSet<String>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		String[] multiParent = parent.split(",");
		for (int m = 0; m < multiParent.length; m++) {
			Calendar cal = Calendar.getInstance();
			// 2014-09-22:10:00:00
			String[] date = child.split("[/:]", 2);
			cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(date[0]));
			cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(date[1]));
			for (int val = 0; val <= type; val++) {
				// 20140920-2311521
				boolean hasAnElement = false;
				String fileNameBeginHour = String.format(
						"%1$tY%1$tm%1$td-%1$tH0000", cal);
				cal.add(Calendar.HOUR_OF_DAY, 1);
				String fileNameEndHour = String.format(
						"%1$tY%1$tm%1$td-%1$tH0000", cal);
				// 查看当前日期内的文件属性
				FileStatus[] status = fs.listStatus(new Path(multiParent[m],
						date[0]), new com.in.util.TmpFileFilter());
				int fileNum = status.length;
				int start = 0, end = 0;

				for (int i = 0; i < fileNum; i++) {
					String fileName = parseFlumeFileName(status[i].getPath()
							.getName());
					// 查看当前日期内的文件属性
					if (fileNameEndHour.compareTo(fileName) < 0) {
						end = i - 1;
						break;
					}

					if (fileNameBeginHour.compareTo(fileName) <= 0) {
						if (!hasAnElement) {
							start = i;
							hasAnElement = true;
						}
						set.add(status[i].getPath().toString());
					}

					if (i == fileNum - 1) {
						end = i;
					}
				}
				// 如果仍然没有找到合适的文件，则将最后的文件作为输入
				if (!hasAnElement) {
					set.add(status[end].getPath().toString());
				}

				// 在当前日期中向前或者向后取1个文件
				if (start > 0) {
					set.add(status[start - 1].getPath().toString());
				}
				if (end < fileNum - 1 && end >= 0) {
					set.add(status[end + 1].getPath().toString());
				}
				if (start == 0 && date[1].equals("00")) {
					// 获取统计日前一天的最后一个压缩文件
					if (fs.exists(new Path(multiParent[m], com.in.util.TimeUtil.addDate(
							date[0], -1)))) {
						status = fs.listStatus(new Path(multiParent[m],
								com.in.util.TimeUtil.addDate(date[0], -1)),
								new com.in.util.TmpFileFilter());
						set.add(status[status.length - 1].getPath().toString());
					}
				}
				if (end == fileNum - 1 && date[1].equals("23")) {
					// 获取统计日后一天的第一个压缩文件
					if (fs.exists(new Path(multiParent[m], com.in.util.TimeUtil.addDate(
							date[0], 1)))) {
						status = fs.listStatus(new Path(multiParent[m],
								com.in.util.TimeUtil.addDate(date[0], 1)),
								new com.in.util.TmpFileFilter());
						if (status.length > 0)
							set.add(status[0].getPath().toString());
					}
				}

				cal.add(Calendar.HOUR_OF_DAY, -2);
				// String tmp = String.format("%1$tY-%1$tm-%1$td/%1$tH", cal);
				date = String.format("%1$tY-%1$tm-%1$td/%1$tH", cal).split(
						"[/:]", 2);
			}
		}
		String sb = set.toString();
		return sb.substring(1, sb.length() - 1);
	}

	/**
	 * 整点统计时添加输入文件。
	 * 
	 * @param job
	 *            Job. Job对象。
	 * @param
	 *
	 * @param child
	 *            String. 统计日期字面值。
	 * @param type
	 *            int. 统计窗口。
	 * @throws java.io.IOException
	 */
	public static void addHourlyInputPaths(Job job, String parent,
			String child, int type) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Set<String> set = new TreeSet<String>();

		String[] multiParent = parent.split(",");
		for (int m = 0; m < multiParent.length; m++) {
			Calendar cal = Calendar.getInstance();
			// 2011-11-22:10:00:00
			String[] date = child.split("[/:]", 2);
			cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(date[0]));
			cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(date[1]));
			for (int val = 0; val <= type; val++) {
				// 20111120-2311521
				boolean hasAnElement = false;
				String fileNameBeginHour = String.format(
						"%1$tY%1$tm%1$td-%1$tH0000", cal);
				cal.add(Calendar.HOUR_OF_DAY, 1);
				String fileNameEndHour = String.format(
						"%1$tY%1$tm%1$td-%1$tH0000", cal);
				// 查看当前日期内的文件属性
				FileStatus[] status = fs.listStatus(new Path(multiParent[m],
						date[0]), new com.in.util.TmpFileFilter());
				int fileNum = status.length;
				int start = 0, end = 0;

				for (int i = 0; i < fileNum; i++) {
					String fileName = parseFlumeFileName(status[i].getPath()
							.getName());
					// 取2者之间的文件
					if (fileNameEndHour.compareTo(fileName) < 0) {
						end = i - 1;
						break;
					}

					if (fileNameBeginHour.compareTo(fileName) <= 0) {
						if (!hasAnElement) {
							start = i;
							hasAnElement = true;
						}
						set.add(status[i].getPath().toString());
					}

					if (i == fileNum - 1) {
						end = i;
					}
				}
				// 如果仍然没有找到合适的文件，则将最后的文件作为输入
				if (!hasAnElement) {
					set.add(status[end].getPath().toString());
				}

				// 在当前日期中向前或者向后取1个文件
				if (start > 0) {
					set.add(status[start - 1].getPath().toString());
				}
				if (end < fileNum - 1 && end > 0) {
					set.add(status[end + 1].getPath().toString());
				}
				if (start == 0 && date[1].equals("00")) {
					// 获取统计日前一天的最后一个压缩文件
					if (fs.exists(new Path(multiParent[m], com.in.util.TimeUtil.addDate(
							date[0], -1)))) {
						status = fs.listStatus(new Path(multiParent[m],
								com.in.util.TimeUtil.addDate(date[0], -1)),
								new com.in.util.TmpFileFilter());
						set.add(status[status.length - 1].getPath().toString());
					}
				}
				if (end == fileNum - 1 && date[1].equals("23")) {
					// 获取统计日后一天的第一个压缩文件
					if (fs.exists(new Path(multiParent[m], com.in.util.TimeUtil.addDate(
							date[0], 1)))) {
						status = fs.listStatus(new Path(multiParent[m],
								com.in.util.TimeUtil.addDate(date[0], 1)),
								new com.in.util.TmpFileFilter());
						if (status.length > 0)
							set.add(status[0].getPath().toString());
					}
				}

				cal.add(Calendar.HOUR_OF_DAY, -2);
				// String tmp = String.format("%1$tY-%1$tm-%1$td/%1$tH", cal);
				date = String.format("%1$tY-%1$tm-%1$td/%1$tH", cal).split(
						"[/:]", 2);
			}
		}
		for (String path : set) {
			LzoTextInputFormat.addInputPath(job, new Path(path));
		}
	}

	/**
	 * @param job
	 *            Job. Job对象。
	 * @param parents
	 *            String. 源路径。
	 * @param child
	 *            String. 统计日期字面值。
	 * @throws java.io.IOException
	 */
	public static void addInputPaths(Job job, String parents, String child)
			throws IOException {
		int type = job.getConfiguration().getInt("stat.window.size", 0);
		if (job.getConfiguration().get("stat.interval.type", "hourly")
				.equals("hourly")) {
			addHourlyInputPaths(job, parents, child, type);
		} else {
			addDailyInputPaths(job, parents, child, type);
		}
	}

	/**
	 * @param job
	 *            Job. Job对象。
	 * @param parents
	 *            String. 源路径。
	 * @param child
	 *            String. 统计日期字面值。
	 * @param type
	 *            int. 整点统计窗口。
	 * @throws java.io.IOException
	 */
	public static void addInputPaths(Job job, String parents, String child,
			int type) throws IOException {
		addHourlyInputPaths(job, parents, child, type);
	}

	/**
	 * 添加MapReduce任务输入路径，默认以一天为区间；若配置了stat.window.size参数，则以此值为区间
	 * 
	 * @param job
	 *            Job. Job对象。
	 * @param parents
	 *            String. HDFS中的日志路径。
	 * @param child
	 *            String. 归档日期，默认是当天的日期。
	 * @throws java.io.IOException
	 */
	public static void addDailyInputPaths(Job job, String parents, String child)
			throws IOException {
		int type = job.getConfiguration().getInt("stat.window.size", 0);
		addDailyInputPaths(job, parents, child, type);
	}

	/**
	 * 添加MapReduce任务输入路径。
	 * 
	 * @param job
	 *            Job. Job对象。
	 * @param parents
	 *            String. HDFS中的某个日志源路径，也可以是多个源目录，用逗号分隔即可。
	 * @param child
	 *            String. 归档日期，默认是当天的日期。
	 * @param type
	 *            int. 统计所使用的时间窗口，默认是一天，也可以是其他数值。
	 * @throws java.io.IOException
	 */
	public static void addDailyInputPaths(Job job, String parents,
			String child, int type) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {

				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					status = fs.listStatus(new Path(parent, child),
							new com.in.util.TmpFileFilter());
					for (FileStatus st : status) {
						set.add(st.getPath().toString());
					}
				}

				// 获取统计日前一天的最后一个压缩文件
				if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, -1)))) {
					status = fs.listStatus(
							new Path(parent, com.in.util.TimeUtil.addDate(child, -1)),
							new com.in.util.TmpFileFilter());
					// 如果前一天存在数据
					if (status != null && status.length > 0) {
						if (status[status.length - 1].getPath().toString()
								.endsWith(".index")) {
							set.add(status[status.length - 2].getPath()
									.toString());
						} else {
							set.add(status[status.length - 1].getPath()
									.toString());
						}
					}
				}
				// 获取统计日后一天的第一个压缩文件
				if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, 1)))) {
					status = fs.listStatus(
							new Path(parent, com.in.util.TimeUtil.addDate(child, 1)),
							new com.in.util.TmpFileFilter());
					if (status != null && status.length > 0) {
						set.add(status[0].getPath().toString());
					}
				}
			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			LzoTextInputFormat.addInputPath(job, new Path(path));
		}
	}

	/**
	 * 获取本地系统目录中特定后缀的文件路径
	 * 
	 * @param path
	 *            本地文件目录
	 * @param suffix
	 *            文件名后缀
	 * @return 返回待查找的第一个特定文件路径
	 */
	public static String getLocalFile(String path, String suffix) {
		String filePath = "";
		File[] files = new File(path).listFiles();
		for (File f : files) {
			if (f.getName().endsWith("." + suffix)) {
				filePath = f.getAbsolutePath();
				break;
			}
		}
		return filePath;
	}

	public static Properties getConfig() {
		if (prop.isEmpty()) {
			log.info("Configuration is Empty!");
			prop = getConfig(System.getProperty("user.dir"));
		}
		return prop;
	}

	public static Properties getConfig(String path) {
		try {
			prop.load(new FileReader(getLocalFile(path, "properties.xml")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prop;
	}

	/**
	 * 添加MapReduce任务输入路径。
	 * 
	 * @param job
	 *            Job. Job对象。
	 * @param parents
	 *            String. HDFS中的某个日志源路径，也可以是多个源目录，用逗号分隔即可。
	 * @param child
	 *            String. 归档日期，默认是当天的日期。
	 * @param type
	 *            int. 统计所使用的时间窗口，默认是一天，也可以是其他数值。
	 * @throws java.io.IOException
	 */
	public static void addDailyLzoInputPaths(Job job, String parents,
			String child, int type, String str) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {

				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					status = fs.listStatus(new Path(parent, child),
							new com.in.util.TmpFileFilter());
					for (FileStatus st : status) {
						if (st.getPath().toString().contains(str)) {
							set.add(st.getPath().toString());
						}
					}
				}

			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			LzoTextInputFormat.addInputPath(job, new Path(path));
		}

	}

	public static void addDailyLzo2InputPaths(Job job, String parents,
			String child, int type, String str1, String str2)
			throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {

				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					status = fs.listStatus(new Path(parent, child),
							new com.in.util.TmpFileFilter());
					for (FileStatus st : status) {
						if (st.getPath().toString().contains(str1)) {
							set.add(st.getPath().toString());
						}
						if (st.getPath().toString().contains(str2)) {
							set.add(st.getPath().toString());
						}
					}
				}
			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			LzoTextInputFormat.addInputPath(job, new Path(path));
		}

	}

	public static void addDailyLzoInputPaths(Job job, String parents,
			String child, int type) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {

				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					status = fs.listStatus(new Path(parent, child),
							new com.in.util.TmpFileFilter());
					for (FileStatus st : status) {
						if (st.getPath().toString().contains("part-r")) {
							set.add(st.getPath().toString());
						}
					}
				}

				// 获取统计日前一天的最后一个压缩文件
				if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, -1)))) {
					status = fs.listStatus(
							new Path(parent, com.in.util.TimeUtil.addDate(child, -1)),
							new com.in.util.TmpFileFilter());
					// 如果前一天存在数据
					if (status != null && status.length > 0) {
						if (status[status.length - 1].getPath().toString()
								.endsWith(".index")) {
							if (status[status.length - 2].getPath().toString()
									.contains("part-r")) {
								set.add(status[status.length - 2].getPath()
										.toString());
							}

						} else {
							if (status[status.length - 1].getPath().toString()
									.contains("part-r")) {
								set.add(status[status.length - 1].getPath()
										.toString());
							}
						}
					}
				}
				// 获取统计日后一天的第一个压缩文件
				if (fs.exists(new Path(parent, com.in.util.TimeUtil.addDate(child, 1)))) {
					status = fs.listStatus(
							new Path(parent, com.in.util.TimeUtil.addDate(child, 1)),
							new com.in.util.TmpFileFilter());
					if (status != null && status.length > 0) {
						if (status[0].getPath().toString().contains("part-r")) {
							set.add(status[0].getPath().toString());
						}
					}
				}
			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			LzoTextInputFormat.addInputPath(job, new Path(path));
		}

	}

	public static void addDailyFileInputPaths(Job job, String parents,
			String child, int type, String str) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {

				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					status = fs.listStatus(new Path(parent, child),
							new com.in.util.TmpFileFilter());
					for (FileStatus st : status) {
						if (st.getPath().toString().contains(str)) {
							set.add(st.getPath().toString());
						}
					}
				}

			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			TextInputFormat.addInputPath(job, new Path(path));
		}

	}

	public static void addWorkDayInputPaths(Job job, String parents,
			String child, int type, String str) throws IOException, ParseException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {
			
				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					
					//获取周末日志
					DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
					String bDate = child;
					Date bdate = format1.parse(bDate);
					Calendar cal2 = Calendar.getInstance();
					cal2.setTime(bdate);
					if (cal2.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
							|| cal2.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
						status = fs.listStatus(new Path(parent, child),
								new com.in.util.TmpFileFilter());
						for (FileStatus st : status) {
							if (st.getPath().toString().contains(str)) {
								set.add(st.getPath().toString());
							}
						}
					} else {
//						status = fs.listStatus(new Path(parent, child),
//								new TmpFileFilter());
//						for (FileStatus st : status) {
//							if (st.getPath().toString().contains(str)) {
//								set.add(st.getPath().toString());
//							}
//						}
					}
	                //
					
					
//					status = fs.listStatus(new Path(parent, child),
//							new TmpFileFilter());
//					for (FileStatus st : status) {
//						if (st.getPath().toString().contains(str)) {
//							set.add(st.getPath().toString());
//						}
//					}
				}

			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			TextInputFormat.addInputPath(job, new Path(path));
		}
	}
	
	public static void addWorkDayInputPaths1(Job job, String parents,
			String child, int type, String str) throws IOException, ParseException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status = null;

		Set<String> set = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(com.in.util.TimeUtil.getDateInLongFormat(child));
		for (int val = 0; val <= type; val++) {
			for (String parent : Arrays.asList(parents.split(","))) {
			
				// 当天的日志数据
				if (fs.exists(new Path(parent, child))) {
					
					//获取周末日志
					DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
					String bDate = child;
					Date bdate = format1.parse(bDate);
					Calendar cal2 = Calendar.getInstance();
					cal2.setTime(bdate);
					if (cal2.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
							|| cal2.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
					
					} else {
						status = fs.listStatus(new Path(parent, child),
								new com.in.util.TmpFileFilter());
						for (FileStatus st : status) {
							if (st.getPath().toString().contains(str)) {
								set.add(st.getPath().toString());
							}
						}
					}
	                //
					
					
//					status = fs.listStatus(new Path(parent, child),
//							new TmpFileFilter());
//					for (FileStatus st : status) {
//						if (st.getPath().toString().contains(str)) {
//							set.add(st.getPath().toString());
//						}
//					}
				}

			}
			cal.add(Calendar.DATE, -1);
			child = String.format("%tF", cal);
		}
		for (String path : set) {
			TextInputFormat.addInputPath(job, new Path(path));
		}
	}

	public static void main(String[] args) throws ParseException {
		DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
		String bDate = "2014-11-20";
		Date bdate = format1.parse(bDate);
		Calendar cal = Calendar.getInstance();
		cal.setTime(bdate);
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
				|| cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			System.out.println("yes");
		} else {
			System.out.println("no");
		}
	}

}
