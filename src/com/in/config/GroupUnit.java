package com.in.config;

import java.util.ArrayList;
import java.util.List;

/**
 * 位于<Stat_unit></stat_unit>中的参数
 * 
 * @author tzl
 *
 */
public class GroupUnit{
	String pvItem;//pv
	String uvItem;//uv
	String Description;//统计描述
	String programName;//项目名称
	String activityDesc;
	String activityName;
	String statID;//统计id
	String statDesc;
	String statType;//统计类型：每日统计/推广统计等
	String unitNum;
	
	public String getStatType() {
		return statType;
	}

	public void setStatType(String statType) {
		this.statType = statType;
	}

	public String getDescription() {
		return Description;
	}

	public void setDescription(String description) {
		Description = description;
	}

	public String getActivityDesc() {
		return activityDesc;
	}

	public void setActivityDesc(String activityDesc) {
		this.activityDesc = activityDesc;
	}

	public String getActivityName() {
		return activityName;
	}

	public void setActivityName(String activityName) {
		this.activityName = activityName;
	}

	public String getStatID() {
		return statID;
	}

	public void setStatID(String statID) {
		this.statID = statID;
	}

	public String getStatDesc() {
		return statDesc;
	}

	public void setStatDesc(String statDesc) {
		this.statDesc = statDesc;
	}

	List<EntryUnit> groupEntry = new ArrayList<EntryUnit>();
	List<String> groupEntryMatchList = new ArrayList<String>();
	
	public void setPvItem(String pvItem) {
		// TODO Auto-generated method stub
		this.pvItem = pvItem;
	}

	public void setUvItem(String uvItem) {
		// TODO Auto-generated method stub
		this.uvItem = uvItem;
	}

	public String getPvItem() {
		// TODO Auto-generated method stub
		return this.pvItem;
	}
	
	public String getUvItem() {
		// TODO Auto-generated method stub
		return this.uvItem;
	}
	
	public void setProgramName(String programName){
		this.programName = programName;
	}
	
	public String getProgramName(){
		return this.programName;
	}
	
	public void addGroupEntry(String fieldName, String matchType, String matchValue) {
		// TODO Auto-generated method stub
		groupEntry.add(new EntryUnit(fieldName,matchType,matchValue));
	}
	
	public List<EntryUnit> getGroupEntry(){
		return this.groupEntry;
	}
	
	public String getUnitNum(){
		return this.unitNum;
	}
	
	public void setUnitNum(String unitNum){
		this.unitNum = unitNum;
	}
	
}
