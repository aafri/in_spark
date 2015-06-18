package com.in.upv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.in.config.GroupUnit;
import com.in.config.MatchUnit;
import com.in.config.StatUnit;
import com.in.config.XMLParser;
import com.in.log.AccessLog;
import com.in.mapred.LogAggrKey;
import com.in.mapred.LogAggrValue;
import com.in.util.Consnt;

public class UPVMapper extends
		Mapper<LongWritable, Text, LogAggrKey, LogAggrValue> {

	private List<StatUnit> list = new ArrayList<StatUnit>();

	// 存储正则捕获组列表
	List<String> group = new ArrayList<String>();
	List<MatchUnit> matchUnit = new ArrayList<MatchUnit>();
	Map<String, List<String>> groupResult = new HashMap<String, List<String>>();

	/**
	 * call at the beginning of the map
	 */

	public void setup(Context context) throws IOException, InterruptedException {

		// 读取统计项配置文件
//		list = XMLParser.retrieveUnits("itugo_upv.xml");
//		list = XMLParser.retrieveUnits("itugo_test.xml");
		list = XMLParser.retrieveUnits("in_app.xml");

	}

	/**
	 * Map过程
	 * */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> fields = AccessLog.parseLog2Map(value.toString()
				.trim(), "ACCESSLOG");
		if (fields != null) {
			processGroupMatching(fields, list, context);
		}

	}

	public void processGroupMatching(Map<String, String> fields,
			List<StatUnit> statUnit, Context context) throws IOException,
			InterruptedException {

		for (StatUnit reduce : statUnit) {
			group.clear();
			// MatchUnit列表
			matchUnit = reduce.getMatchUnit();
			// 处理包含group标签的统计单元
			if (reduce.getUnitType().equals("group")) {

			}
			// 兼容旧的配置，即对一条日志，需要匹配所有的配置项
			else {
				boolean flag = false;
				for (MatchUnit mu : matchUnit) {
					if (mu.matches(fields)) {
						flag = true;
						break;
					}
				}
				if (flag) {
					context.write(
							new LogAggrKey(new Text(getKeyString(reduce)),
									new Text(fields.get(Consnt.LOGITEM_AK))),
							new LogAggrValue(new Text(fields
									.get(Consnt.LOGITEM_AK)), new LongWritable(
									1)));
				}
			}
		}
	}

	private static String getKeyString(GroupUnit gu) {
		String key;

		if (gu.getStatType().equals("daily")) {
			key = gu.getUnitNum() + Consnt.SPLITTER_INNERKEY + gu.getStatType()
					+ Consnt.SPLITTER_INNERKEY + gu.getPvItem()
					+ Consnt.SPLITTER_INNERKEY + gu.getUvItem()
					+ Consnt.SPLITTER_INNERKEY + gu.getDescription();
		} else {
			key = gu.getUnitNum() + Consnt.SPLITTER_INNERKEY + gu.getStatType()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatID()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatDesc()
					+ Consnt.SPLITTER_INNERKEY + gu.getActivityName()
					+ Consnt.SPLITTER_INNERKEY + gu.getActivityDesc()
					+ Consnt.SPLITTER_INNERKEY + gu.getProgramName();
		}
		return key;
	}

	

	

}
