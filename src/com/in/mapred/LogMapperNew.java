package com.in.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

import com.in.config.GroupUnit;
import com.in.config.MatchUnit;
import com.in.config.StatUnit;
import com.in.config.XMLParser;
import com.in.log.ApacheLog;
import com.in.util.Consnt;
import com.in.util.StringUtil;
import com.in.util.TimeUtil;

/**
 * @author tzl
 * @since 2014/09/11
 */

/**
 *  用于计算网站日志中各请求url的点击PV/UV数的Map过程。<br/>
 * 	主要的逻辑流程为：
 * 	<ul>
 *  <li>在对日志进行匹配之前，先对其进行过滤，主要是过滤一些静态图片请求，爬虫日志以及特定指出不计入统计的请求。
 *  <li>对于日志中的每一条记录，需要与统计项配置文件中的每个计算单元做匹配。改进后的统计单元使用了组匹配规则，组内的成员项也是单独的计算单元。
 *  <li>对于规则的Apache日志，一般可以切分成IP、时间戳、请求、来源、域名、服务器cookie、浏览器类型等，具体的切分规则可见 {@link LogRegex}
 *  </ul>
 *  
 */
//public class LogMapperNew extends Mapper<LongWritable, Text, Text, Text> {
public class LogMapperNew extends Mapper<LongWritable, Text, LogAggrKey, LogAggrValue> {

	private List<StatUnit> list = new ArrayList<StatUnit>();
	private List<MatchUnit> filter = new ArrayList<MatchUnit>();
	private String statDateStr = "";

	// 存储正则捕获组列�?
	List<String> group = new ArrayList<String>();
	List<MatchUnit> matchUnit = new ArrayList<MatchUnit>();
	Map<String,List<String>> groupResult = new HashMap<String,List<String>>();

	static ObjectMapper obj = new ObjectMapper();

	/**
	 * Map过程的初始化，读取配置文件，获取命令行传递过来的参数
	 * 
	 * @Override
	 */
	public void setup(Context context) throws IOException, InterruptedException {
		// 读取统计项配置文件
		list = XMLParser.retrieveUnits(System.getProperty("user.dir")
				+ "/" + context.getConfiguration().get(Consnt.STAT_XML_FILE));
		// 读取过滤规则
		filter = XMLParser.retrieveFilterUnits(System.getProperty("user.dir")
				+ "/" + context.getConfiguration().get(Consnt.FILTER_XML_FILE));
		// 读取从命令行传�?来的日期参数，再根据是否是整点统计对日期时间进行格式变更
		statDateStr = context.getConfiguration().get("stat.date.str");
		if(context.getConfiguration().get("stat.interval.type","hourly").equals("hourly")){
			// 2014-09-18/14 => 18/SEP/2014:14
			statDateStr = statDateStr.substring(8, 10) + "/"
					+ TimeUtil.MonInNumStyle.get(statDateStr.substring(5, 7))
					+ "/" + statDateStr.substring(0, 4) + ":"
					+ statDateStr.substring(11);
		}else{
			// 2014-09-18 => 18/SEP/2014
			statDateStr = statDateStr.substring(8, 10) + "/"
					+ TimeUtil.MonInNumStyle.get(statDateStr.substring(5, 7))
					+ "/" + statDateStr.substring(0, 4);
		}	
	}

	/**
	 * Map过程
	 * @param args
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String log = value.toString().trim();
		if(log.startsWith("{")){
			try{
				if(!obj.readTree(log).path("body").isMissingNode())
					log = obj.readTree(log).path("body").getTextValue().trim();
			}catch(JsonParseException e){}
		}
		Map<String, String> fields = ApacheLog.parseLog2Map(log, context.getConfiguration().get(Consnt.STAT_LOG_TYPE));
		if (fields.containsKey(Consnt.LOGITEM_TIMESTAMP)
				&& fields.get(Consnt.LOGITEM_TIMESTAMP).startsWith(statDateStr)
				&& fields.get(Consnt.LOGITEM_REQ).indexOf("${") == -1) {

			// remove multi-slashs in request
			String request = fields.get(Consnt.LOGITEM_REQ);
			if(request.contains(" //")){
				int slash = request.indexOf("/");
				int next = slash;
				while(slash != -1 && request.charAt(next) == '/'){
					next ++;
				}	
				fields.put(Consnt.LOGITEM_REQ,request.substring(0,slash) + request.substring(next-1));
			}
			if(filter != null && ApacheLog.filterGroup(filter, fields)){
				// do nothing
			}else{
				processGroupMatching(fields, list, context);
			}
		}
	}

	/**
	 * 新增加的用于计算包含规则组的处理过程。其改进的初衷是：
	 * <ul>
	 * <li>对于满足一类正则的url匹配规则来说，如果对于一条日志不能匹配上，则其他相似的规则同样也不能匹配上该条日志，故在匹配时能考虑到这种因素
	 * 进入减少不必要的匹配次数，对于整个程序的效率将会大大提高。</li>
	 * </ul>
	 * 在使用组配置规则后，对于符合相同正则的匹配规则可以合并到一个组中，通过使用正则中的捕获组来获取特异性的字段/值，进而再使用这个值在组内的规则
	 * 中进行匹配。在组内匹配的规则方式和在组外的匹配规则不一样，主要表现在：
	 * <ul>
	 * <li>一般只有一个匹配规则，规则有3类：
	 * <ul>
	 * <li>&lt;entry field_name="1" match_type="equal"&gt;&lt;![CDATA[a]]&gt;&lt;/entry&gt;</li>
	 * <li>&lt;entry field_name="1" match_type="match"&gt;&lt;![CDATA[[abc]]]&gt;&lt;/entry&gt;</li>
	 * <li>&lt;entry field_name="1,2,3" match_type="combine"&gt;&lt;![CDATA[a b c]]&gt;&lt;/entry&gt;</li>
	 * </ul>
	 * 1、对于equal类型，在程序加载配置规则时会将该组的相关规则汇集到一个List列表中，然后通过直接通过索引该值即可<br/>
	 * 2、对于match类型，程序同样在加载配置时将其汇总至一个List列表，但需要对整个列表进行遍历操作<br/>
	 * 3、对于combine类型，程序加载后遍历一个Map对象，根据field_name中的组编号对捕获的值进行组合，然后再进行equal匹配，同样需要遍历整个Map<br/>
	 * 上述1、2类型只对匹配序列中第一个组有效（推荐使用第一个组，且同样类型只能有1个），所以配置时必须注意，对于不用捕获的组，用?:注释掉<br/>
	 * </li>
	 * </ul>
	 * 综上，对于Group中成员的规则配置，尽量使用equal规则，这样能最大化减少匹配次数，提高效率。如果单独一个正则捕获组无法解决需求，可以使用多个组通过
	 * combine类型的equal匹配规则来实现。match类型适合多种值聚合后的统计类型。
	 * 
	 * 
	 * @param fields 经过正则表达式解析后的日志各分段的map对象
	 * @param statUnit 统计项匹配规则列表
	 * @param context
	 * @throws java.io.IOException
	 * @throws InterruptedException
	 */
	public  void processGroupMatching(Map<String, String> fields,
			List<StatUnit> statUnit, Context context) throws IOException,
			InterruptedException {
		for(StatUnit reduce : statUnit){
			group.clear();
			// MatchUnit列表
			matchUnit = reduce.getMatchUnit();
			// 提取用于去重的字段，默认是IP,如果没有设置extract标签，则直接使用IP段
			String unique;
			if(reduce.getExtractor().equals("")){
				unique = fields.get(Consnt.LOGITEM_IP);
			}else{
				unique = reduce.extractUniqueKey(fields.get(Consnt.LOGITEM_ALL));
			}
			// 处理包含group标签的统计单元
			if(reduce.getUnitType().equals("group")){
				// group所用的MatchUnit基本上只有1个,如果存在多个，则以第二个MatchUnit为准
				for(MatchUnit mu : matchUnit){
					group = mu.groupMatches(fields);
				}

				// 之前考虑到在成员配置中使用多个配置，故需要额外使用一个List对象，目前已改为只使用一个配置，这里仍保存旧制。
				// 但这个List对象基本上不需后续使用，如后续考虑需要使用，可以续用。
				// 每次清空整个Map
				groupResult.clear();
				// 存在捕获组的情况
				if(group.size()>0){
					// 如果StatUnit中存在全�??匹配规则，则直接将此次成功的匹配计入
					if(!reduce.getPvItem().equals("")||!reduce.getUvItem().equals("")||!reduce.getStatID().equals("")){
						//context.write(new Text(unique),new Text(getKeyString(reduce)+"\t1"));
						context.write(new LogAggrKey(new Text(getKeyString(reduce)),new Text(unique)),new LogAggrValue(new Text(unique),new LongWritable(1)));
					}

					for(int index=0;index<group.size();index++){
						// 处理equal匹配规则
						if(reduce.getGlobalMatch().containsKey((index+1)+"EQUAL")){
							String key = (index+1)+"EQUAL";
							// 当组内的全局Map对象中存在
							if(reduce.getGlobalMatch().get(key).getMatchValue().contains(group.get(index))){
								// 获取规则列表中的序号
								int groupIndex = reduce.getGlobalMatch().get(key).getMatchValue().indexOf(group.get(index));
								// 获得组成员序号，用于提取成员配置信息
								int finalIndex = Integer.parseInt(reduce.getGlobalMatch().get(key).getGroupIndex().get(groupIndex));
								if(groupResult.containsKey(Integer.toString(finalIndex))){
									groupResult.get(Integer.toString(finalIndex)).add(key);
								}else{
									List<String> list = new ArrayList<String>();
									list.add(key);
									groupResult.put(Integer.toString(finalIndex), list);
								}
							}
						}

						// match类型需要做完全遍历 
						if(reduce.getGlobalMatch().containsKey((index+1)+"MATCH")){
							String key = (index+1)+"MATCH";
							List<Object> matchMap = reduce.getGlobalMatch().get(key).getMatchValue();
							for(int i=0;i<matchMap.size();i++){
								Matcher matcher = ((Pattern)matchMap.get(i)).matcher(group.get(index));
								if(matcher.find()){	
									int finalIndex = Integer.parseInt(reduce.getGlobalMatch().get(key).getGroupIndex().get(i));		
									if(groupResult.containsKey(Integer.toString(finalIndex))){
										groupResult.get(Integer.toString(finalIndex)).add(key);
									}else{
										List<String> list = new ArrayList<String>();
										list.add(key);
										groupResult.put(Integer.toString(finalIndex), list);
									}
								}
							}
						}
					}	

					// 对combine类型做匹配，同样需要考虑遍历，对其field_name段进行切分后，以此取之对应的值组合后再做匹配
					for(String s : reduce.getGlobalMatch().keySet()){
						if(s.endsWith("COMBINE")){
							String valueCombined = "";						
							String[] a = StringUtil.split(s.substring(0, s.indexOf("COMBINE")),",");
							for(int i=0;i<a.length;i++){
								if(valueCombined.length()>0){
									valueCombined += " ";
								}
								if(Integer.parseInt(a[i]) <= group.size()){
									valueCombined += group.get(Integer.parseInt(a[i])-1);
								}else{
									valueCombined += "";
								}
							}
							if(reduce.getGlobalMatch().get(s).getMatchValue().contains(valueCombined)){
								int groupIndex = reduce.getGlobalMatch().get(s).getMatchValue().indexOf(valueCombined);
								int finalIndex = Integer.parseInt(reduce.getGlobalMatch().get(s).getGroupIndex().get(groupIndex));
								if(groupResult.containsKey(Integer.toString(finalIndex))){
									groupResult.get(Integer.toString(finalIndex)).add(s);
								}else{
									List<String> list = new ArrayList<String>();
									list.add(s);
									groupResult.put(Integer.toString(finalIndex), list);
								}
							}
						}
					}

					// 根据匹配到的组，输出其配置信息
					for(String key : groupResult.keySet()){
						GroupUnit gu = reduce.getGroupUnit().get(Integer.parseInt(key));
						//context.write(new Text(unique),new Text(getKeyString(gu)+"\t1"));
						context.write(new LogAggrKey(new Text(getKeyString(gu)),new Text(unique)),new LogAggrValue(new Text(unique),new LongWritable(1)));
					}
				}
			}
			// 兼容旧的配置，即对一条日志，需要匹配所有的配置项
			else {
				boolean flag = false;
				for(MatchUnit mu : matchUnit){
					if(mu.matches(fields)){
						flag = true;
						break;
					}
				}
				if(flag){
					//context.write(new Text(unique),new Text(getKeyString(reduce)+"\t1"));
					context.write(new LogAggrKey(new Text(getKeyString(reduce)),new Text(unique)),new LogAggrValue(new Text(unique),new LongWritable(1)));
				}
			}
		}
	}

	private static String getKeyString(GroupUnit gu){	
		String key;
		if(gu.getStatType().equals("daily")){
			key = gu.getUnitNum()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatType()	
					+ Consnt.SPLITTER_INNERKEY + gu.getPvItem()
					+ Consnt.SPLITTER_INNERKEY + gu.getUvItem();
		}else{
			key = gu.getUnitNum()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatType()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatID()
					+ Consnt.SPLITTER_INNERKEY + gu.getStatDesc()
					+ Consnt.SPLITTER_INNERKEY + gu.getActivityName()
					+ Consnt.SPLITTER_INNERKEY + gu.getActivityDesc()
					+ Consnt.SPLITTER_INNERKEY + gu.getProgramName();
		}
		return key;
	}
}
