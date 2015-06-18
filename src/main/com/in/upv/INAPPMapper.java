package main.com.in.upv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.in.config.EntryUnit;
import com.in.config.GroupUnit;
import com.in.config.MatchUnit;
import com.in.config.StatUnit;
import com.in.config.XMLParser;
import com.in.log.AccessLog;
import com.in.mapred.ResponseValue;
import com.in.util.Consnt;

/**
 * 用于计算网站日志中各请求url的点击PV/UV数的Map过程。 主要的逻辑流程为：
 * <ul>
 * <li>在对日志进行匹配之前，先对其进行过滤，主要是过滤一些静态图片请求，爬虫日志以及特定指出不计入统计的请求。
 * <li>对于日志中的每一条记录，需要与统计项配置文件中的每个计算单元做匹配。改进后的统计单元使用了组匹配规则，组内的成员项也是单独的计算单元。
 * <li>对于规则的日志，一般可以切分成IP、时间戳、请求、来源、域名、服务器cookie、浏览器类型等
 * </ul>
 * 
 * 
 */

public class INAPPMapper extends
		Mapper<LongWritable, Text, Text, ResponseValue> {

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
//		list = XMLParser.retrieveUnits("itugo_test1.xml");
		list = XMLParser.retrieveUnits("itugo_test1_cp.xml");

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
					
					Pattern p=Pattern.compile("\\d+.\\d+");
					Matcher m=p.matcher(fields
							.get(Consnt.LOGITEM_RESPONSE));
                    if(m.matches()){
					context.write(
							new Text(getKeyString(reduce)),
							new ResponseValue(new Text(fields
									.get(Consnt.LOGITEM_STATE)),
									new DoubleWritable(Double.valueOf(fields
											.get(Consnt.LOGITEM_RESPONSE)))));
                    }
				}
			}
		}
	}

	private static String getKeyString(GroupUnit gu) {
		String key;

		if (gu.getStatType().equals("daily")) {
			key = 
//					gu.getUnitNum() + Consnt.SPLITTER_INNERKEY + gu.getStatType()
//					+ Consnt.SPLITTER_INNERKEY + gu.getPvItem()
//					+ Consnt.SPLITTER_INNERKEY + gu.getUvItem()
//					+ 
//					Consnt.SPLITTER_INNERKEY + 
					gu.getDescription();
		} else {
			key = 
//					gu.getUnitNum() + Consnt.SPLITTER_INNERKEY + gu.getStatType()
//					+ Consnt.SPLITTER_INNERKEY + gu.getStatID()
//					+ Consnt.SPLITTER_INNERKEY + gu.getStatDesc()
//					+ Consnt.SPLITTER_INNERKEY + gu.getActivityName()
//					+ Consnt.SPLITTER_INNERKEY + gu.getActivityDesc()
//					+ 
//					Consnt.SPLITTER_INNERKEY 
//					+ 
					gu.getDescription();
		}
		return key;
	}

	public static void main(String[] args) {

		String value = "171.94.128.239 - - [02/Sep/2014:00:00:03 +0800] \"GET /shopping/mobile?itemid=37659793422&y_url=http%3A%2F%2Fitem.taobao.com%2Fitem.htm%3Fid%3D37659793422 HTTP/1.1\" 302 5 \"http://link.itugo.com/?g=http%3A%2F%2Fm.itugo.com%2Fshopping%2Fmobile%3Fitemid%3D37659793422%26y_url%3Dhttp%253A%252F%252Fitem.taobao.com%252Fitem.htm%253Fid%253D37659793422\" \"Mozilla/5.0 (Linux; U; Android 4.1.2; zh-CN; HUAWEI C8815 Build/HuaweiC8815) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 UCBrowser/9.9.2.467 U3/0.8.0 Mobile Safari/533.1\" m.itugo.com 2btvilge2um2ktjjon5l2dv9c4 - - weibo_n0901_fy_wbnrppz_0.68189600_1409587177 - ef125530d6897075d0d69273fbd96385 0.138";
		Map<String, String> fields = AccessLog.parseLog2Map(value.toString()
				.trim(), "ACCESSLOG");
		System.out.println(fields.size());

		List<StatUnit> slist = XMLParser.retrieveUnits("f://itugo_test.xml");
		System.out.println(slist.size());

		// 存储正则捕获组列表
		List<String> group = new ArrayList<String>();
		List<MatchUnit> matchUnit = new ArrayList<MatchUnit>();

		for (StatUnit reduce : slist) {
			System.out.println("in");
			group.clear();
			// MatchUnit列表
			matchUnit = reduce.getMatchUnit();
			System.out.println(matchUnit.get(0).getCombineType());
			// 提取用于去重的字段，默认是IP,如果没有设置extract标签，则直接使用IP段
			String unique;
			if (reduce.getExtractor().equals("")) {
				unique = fields.get(Consnt.LOGITEM_IP);
			} else {
				unique = reduce
						.extractUniqueKey(fields.get(Consnt.LOGITEM_ALL));
			}
			System.out.println(unique);

			System.out.println(reduce.getExtractor());
			System.out.println(reduce.getActivityName());
			System.out.println(reduce.getActivityDesc());
			System.out.println(reduce.getPvItem());
			System.out.println(reduce.getUnitType().toString());
			System.out.println(reduce.getDescription());
			System.out.println("before if");

			// 处理包含group标签的统计单元
			if (reduce.getUnitType().equals("group")) {
			}
			// 兼容旧的配置，即对一条日志，需要匹配所有的配置项
			else {
				System.out.println("no unittype group");
				boolean flag = false;
				for (MatchUnit mu : matchUnit) {
					System.out.println("mu in");

					System.out.println(reduce.getDescription());

					List<EntryUnit> eu = mu.getEntryUnit();
					System.out.println("eu.size():" + eu.size());

					String matchType = "";
					for (int i = 0; i < eu.size(); i++) {
						EntryUnit f = eu.get(i);
						matchType = f.matchValue + "@:@" + matchType;
						System.out.println(f.matchType);
						System.out.println("haha:" + f.matchValue);
					}

					System.out.println("matchType:" + matchType);

					System.out.println(eu.get(0));

					if (mu.matches(fields)) {
						System.out.println("??");
						flag = true;
						break;
					}
				}
				System.out.println(flag);
				if (flag) {
					System.out.println(getKeyString(reduce).toString());

					System.out.println("unique:" + unique);
					// context.write(new LogAggrKey(new
					// Text(getKeyString(reduce)),new Text(unique)),new
					// LogAggrValue(new Text(unique),new LongWritable(1)));
				}
			}
		}

	}

}
