package com.in.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 统计项组单元配置类，为了兼容之前的配置，继承了<code>GroupUnit</code>,并有一个用于在成员单元之间共享全局配置的Map对象。
 * 
 * 对Extract部分在初始化时做调整，将Pattern变量公用，减少每次的编译次数
 * @author Tang Zeliang
 *
 */
public class StatUnit extends GroupUnit{
	private String action;
	private String unitType;
	private String extractor;

	private List<MatchUnit> matchUnit = new ArrayList<MatchUnit>();
	private List<GroupUnit> groupUnit = new ArrayList<GroupUnit>();
	private Map<String,GroupGlobalMatch> globalMatch = new HashMap<String,GroupGlobalMatch>();

	private Pattern pattern = null;
	private Matcher m = null;
	
	public StatUnit(String action) {
		this.action = action;
	}

	protected String getAction() {
		return action;
	}

	protected void setAction(String action) {
		this.action = action;
		
	}

	public String getUnitType() {
		return unitType;
	}

	public void setUnitType(String unitType) {
		this.unitType = unitType;
	}

	public String getExtractor() {
		return extractor;
	}

	public void setExtractor(String extractor) {
		this.extractor = extractor;
		if(!extractor.equals("")){
			pattern = Pattern.compile(extractor);
		}
	}

	public List<MatchUnit> getMatchUnit() {
		return matchUnit;
	}

	public void setMatchUnit(List<MatchUnit> matchUnit) {
		this.matchUnit = matchUnit;
	}

	public List<GroupUnit> getGroupUnit() {
		return groupUnit;
	}

	public void setGroupUnit(List<GroupUnit> groupUnit) {
		this.groupUnit = groupUnit;
	}

	public Map<String, GroupGlobalMatch> getGlobalMatch() {
		return globalMatch;
	}

	public void setGlobalMatch(Map<String, GroupGlobalMatch> globalMatch) {
		this.globalMatch = globalMatch;
	}

	/**
	 * 根据extract的正则表达式提取用于去重的值，如果为空或者不存在，则默认使用IP段
	 * @param line
	 * @return
	 */
	public String extractUniqueKey(String line) {
		// TODO Auto-generated method stub
		if(!extractor.equals("")){
			m = pattern.matcher(line);
			if(m.find()){
				return m.group(1);
			}
		}
		return line.split("\\s+")[0];
	}
}
