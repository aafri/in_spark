package main.com.in.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.in.config.*;
import org.apache.log4j.Logger;

import com.in.util.Consnt;


/**
 * Match规则匹配。一个统计单元中可以存在多个match规则，每个规则之间是或的关系，match里面可以通过指定combineType来限定
 * 规则里面的条件是否是与/或关系。
 * @author Tang Zeliang
 *
 */
public class MatchUnit {
	
	static Logger logger = Logger.getLogger(MatchUnit.class);
	
	String combineType;
	public String getCombineType() {
		return combineType;
	}

	public void setCombineType(String combineType) {
		this.combineType = combineType;
	}

	String groupMatchIndex;
	List<com.in.config.EntryUnit> entryUnit = new ArrayList<com.in.config.EntryUnit>();
	
	List<String> group = new ArrayList<String>();
	
	public MatchUnit(String combineType, String groupMatchIndex) {
		// TODO Auto-generated constructor stub
		this.combineType = combineType;
		this.groupMatchIndex = groupMatchIndex;
	}

	public MatchUnit() {
		// TODO Auto-generated constructor stub
	}

	public void addEntry(String fieldName, String matchType, String matchValue) {
		// TODO Auto-generated method stub
		entryUnit.add(new com.in.config.EntryUnit(fieldName,matchType,matchValue));
	}
	
	public List<com.in.config.EntryUnit> getEntryUnit(){
		return entryUnit;
	}
	
	/**
	 * 根据匹配规则组对日志进行匹配，如果该规则组设置为使用组匹配策略，则会返回根据指定的正则表达式返回的捕获组列表。否则返回一个空列表。
	 * @param fields 经过正则匹配后各字段的Map对象
	 * @return 返回正则表达式的捕获组列表对象
	 */
	public List<String> groupMatches(Map<String,String> fields){
		boolean valid = true;
		if(combineType.equals(Consnt.MATCHCOMBINETYPE_AND)){
			for(com.in.config.EntryUnit eu : entryUnit){
				if(eu.matches(fields) == false){
					valid = false;
					break;
				}
			}
		}else if(combineType.equals(Consnt.MATCHCOMBINETYPE_OR)){
			for(com.in.config.EntryUnit eu : entryUnit){
				if(eu.matches(fields) == true){
					valid = true;
					break;
				}
			}
		}
		if(valid && (groupMatchIndex != null)){
			if(entryUnit.size() >= Integer.parseInt(groupMatchIndex)){
				group = entryUnit.get(Integer.parseInt(groupMatchIndex)-1).getGroupList(fields);
			}
		}
		return group;
	}
	
	public boolean matches(Map<String,String> fields){
		if(combineType.equals(Consnt.MATCHCOMBINETYPE_AND)){
			logger.info("one");
			for(com.in.config.EntryUnit eu : entryUnit){
				logger.info("one2");
				System.out.println("one1");
				if(eu.matches(fields) == false){
					System.out.println("one2");
					return false;
				}
			}
			return true;
		}else if(combineType.equals(Consnt.MATCHCOMBINETYPE_OR)){
			logger.info("two");
			System.out.println("two");
			for(com.in.config.EntryUnit eu : entryUnit){
				if(eu.matches(fields) == true){
					return true;
				}
			}
		}
		return false;
	}
}
