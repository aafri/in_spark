package com.in.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * XML配置文件解析
 * 
 * @author Tang Zeliang
 */
public class XMLParser {
	
	private static Logger logger = Logger.getLogger(XMLParser.class);
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private static Calendar statDate = Calendar.getInstance();
	private static int unitNum = 0;
	
	/**
	 * 用于提取统计项匹配规则
	 * @param xmlFile	xml配置文件全路径(文件需由UTF-8编码)
	 *          
	 * @return List&lt;GroupStatUnit&gt;	返回所有的配置单元组
	 */
	public static List<StatUnit> retrieveUnits(String xmlFile) {
		List<StatUnit> groupUnitList = new ArrayList<StatUnit>();
		try{
			Element root = XMLParser.getRootElementFromXML(xmlFile);
			for (Iterator<?> statUnitIterator = root.elementIterator("stat_unit");statUnitIterator.hasNext();) {
				Element eUnit = (Element)statUnitIterator.next();

				if(checkValidStatUnit(eUnit)){
					continue;
				}

				String action = getTextWithOutNull(eUnit,"action");
				String unitType = getTextWithOutNull(eUnit,"unit_type");
				StatUnit sUnit = new StatUnit(action);
				sUnit.setUnitType(unitType);
				sUnit.setExtractor(getTextWithOutNull(eUnit,"extract"));
				
				XMLParser.propulateProperties(eUnit, sUnit);		
				XMLParser.getMatchItemFromStatUnit(eUnit,sUnit);
				XMLParser.getGroupItemFromStatUnit(eUnit,sUnit);
		
				groupUnitList.add(sUnit);
			}
		}catch(DocumentException e){
			logger.error(e);
		}catch(FileNotFoundException e){
			logger.error(e);
		}
		return groupUnitList;
	}


	/**
	 * 读取过滤规则的xml配置文件
	 * 
	 * @param xmlFile 包含过滤规则的配置文件
	 * @return	包含所有match规则的一个列表
	 */
	public static List<MatchUnit> retrieveFilterUnits(String xmlFile) {
		// TODO Auto-generated method stub
		List<MatchUnit> list = new ArrayList<MatchUnit>();
		try {	
			Element root = XMLParser.getRootElementFromXML(xmlFile);
			for(Object obj1 : root.selectNodes("match")){
				Element matchElement = (Element)obj1;
				MatchUnit match = new MatchUnit();
				match.setCombineType(matchElement.attributeValue("combine_type"));
				for(Object obj2 : matchElement.selectNodes("entry")){
					Element entryElement = (Element)obj2;
					//System.out.println(entryElement.attributeValue("field_name")+"\t"+entryElement.attributeValue("match_type")+"\t"+entryElement.getTextTrim());
					match.addEntry(entryElement.attributeValue("field_name"),
							entryElement.attributeValue("match_type"),
							entryElement.getTextTrim());
				}
				list.add(match);
			}
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		} catch (FileNotFoundException e){
			logger.error(e);
		}
		return list;
	}

	/**
	 * 使用Dom4j读取一个UTF-8编码的XML文件，并返回其根节点的Element对象
	 * 
	 * @param xmlFile	xml配置文件的全路径
	 * @return	返回根节点的Element对象
	 * @throws org.dom4j.DocumentException
	 */
	public static Element getRootElementFromXML(String xmlFile) throws DocumentException,FileNotFoundException {
		SAXReader saxReader = new SAXReader();
		saxReader.setEncoding("UTF-8");
		Document doc = saxReader.read(new File(xmlFile));
		return doc.getRootElement();
	}

	/**
	 * 遍历group组的配置规则。一个全局的match配置可以有多个group配置，group配置规则会对匹配到的数据在进行细分，
	 * 这样可以减少对于满足同一规则的日志进行多次匹配。

	 * 对于满足正则规则的url，通过定义捕获组来直接得到对应的部位的特异性值，然后再通过简单的key/value
	 * 来获得对应的配置项或者通过较少的遍历来进行对应即可。
	 * 
	 * @param unit	一个包含了groupTag组的配置单元
	 *	
	 */
	private static void getGroupItemFromStatUnit(Element unit,StatUnit statUnit){

		List<GroupUnit> groupList = statUnit.getGroupUnit();
		Map<String,GroupGlobalMatch> map = statUnit.getGlobalMatch();
		int index = 0;
		for (Iterator<?> groupIterator = unit.elementIterator("group");groupIterator.hasNext();) { // match level
			Element groupElement = (Element)groupIterator.next();
			if(checkValidStatUnit(groupElement)){
				continue;
			}
			GroupUnit groupUnit = new GroupUnit();

			XMLParser.propulateProperties(groupElement, groupUnit);

			for (Iterator<?> entryIterator = groupElement.elementIterator("entry");entryIterator.hasNext();) { // entry level
				Element entryElement = (Element)entryIterator.next();
				String fieldName = entryElement.attributeValue("field_name");		
				String matchType = entryElement.attributeValue("match_type");
				String value = entryElement.getTextTrim();
				groupUnit.addGroupEntry(fieldName, matchType, value);			
				String key = (fieldName+matchType).toUpperCase();
				groupUnit.groupEntryMatchList.add(key);
				Object obj = null;
				if(matchType.equals("match")){
					obj = Pattern.compile(value);
				}else{
					obj = value;
				}
				if(map.containsKey(key)){
					map.get(key).add(obj,Integer.toString(index));
				}else{
					map.put(key, new GroupGlobalMatch(obj,Integer.toString(index)));
				}
			}
			index ++;
			groupList.add(groupUnit);
		}
	}

	/**
	 * 遍历配置单元，读取match中对于各个项目的匹配规则，然后将其存入列表。
	 * 如果其中存在多条match，则各match规则之间是或的关系。其标准的格式如下：
	 * 
	 * @param unit 一个包含了match部分的配置单元
	 * @return	返回包含所有match配置的一个List列表
	 */
	private static void getMatchItemFromStatUnit(Element unit,StatUnit statUnit){
		List<MatchUnit> matchItemList = statUnit.getMatchUnit();
		for (Iterator<?> matchIterator = unit.elementIterator("match");matchIterator.hasNext();) { // match level
			Element matchElement = (Element)matchIterator.next();
			String combineType = matchElement.attributeValue("combine_type");
			String groupMatchIndex = matchElement.attributeValue("group_match_index");
			MatchUnit matchItem = new MatchUnit(combineType,groupMatchIndex);
			for (Iterator<?> entryIterator = matchElement.elementIterator("entry");entryIterator.hasNext();) { // entry level
				Element entryElement = (Element)entryIterator.next();
				String fieldName = entryElement.attributeValue("field_name");		
				String matchType = entryElement.attributeValue("match_type");
				String value = entryElement.getTextTrim();
				matchItem.addEntry(fieldName, matchType, value);
			}
			matchItemList.add(matchItem);
		}
	}

	/**
	 * 根据配置单元中是否出现<end_time>tag来判断该配置是否还需要继续计算，标准的时间格式是yyyy-MM-dd
	 * 其中顶层的配置判断优先级最高。
	 * 
	 * @param unit 一个XML统计单元
	 * @return	true则过滤该项配置，反之则采用
	 */
	private static boolean checkValidStatUnit(Element unit){
		Element endTime = unit.element("end_time");
		if (endTime != null) {
			try {
				long deadLine = df.parse(endTime.getTextTrim()).getTime();
				long now = statDate.getTimeInMillis();
				return deadLine < now;
			}catch(ParseException e){
				logger.error(e);
			}
		}
		return false;
	}
	
	/**
	 * 对配置单元中的各个属性赋值
	 * @param eUnit
	 * @param gu
	 */
	public static void propulateProperties(Element eUnit,GroupUnit gu){
		String pvItem = getTextWithOutNull(eUnit,"pvitem");
		String uvItem = getTextWithOutNull(eUnit,"uvitem");	
		String programName = getTextWithOutNull(eUnit,"program_name");
		String statDesc = getTextWithOutNull(eUnit,"stat_desc");
		String statID = getTextWithOutNull(eUnit,"stat_id");
		String activityDesc = getTextWithOutNull(eUnit,"activity_desc");
		String activityName = getTextWithOutNull(eUnit,"activity_name");
		String statType = getTextWithOutNull(eUnit,"stat_type");
		String description=getTextWithOutNull(eUnit,"description");
		
		if(!pvItem.equals("") || !uvItem.equals("") || !statID.equals("")){
			unitNum ++;
			if(!pvItem.equals("") || !uvItem.equals("")){
				statType = "daily";
			}else{
				statType = "activity";
			}
		}

		gu.setUnitNum(Integer.toString(unitNum));
		gu.setPvItem(pvItem);
		gu.setUvItem(uvItem);
		gu.setStatDesc(statDesc);
		gu.setStatID(statID);
		gu.setActivityDesc(activityDesc);
		gu.setActivityName(activityName);
		gu.setProgramName(programName);
		gu.setStatType(statType);
		gu.setDescription(description);
	}
	
	/**
	 * if the value of this tag is null,then return a empty string instead
	 * @param eUnit
	 * @param tagName
	 * @return
	 */
	public static String getTextWithOutNull(Element eUnit, String tagName){
		return eUnit.elementTextTrim(tagName) == null ? "" : eUnit.elementTextTrim(tagName);
	}
}
