package main.com.in.config;

import java.util.ArrayList;
import java.util.List;

/**
 * 组配置统计项中的全局规则对象。存储group规则中的组编号和组规则
 * @author Tang Zeliang
 *
 */
public class GroupGlobalMatch {
	// 组成员entry规则的值
	List<Object> matchValue = new ArrayList<Object>();
	// 组成员编号列表
	List<String> groupIndex = new ArrayList<String>();;
	
	public List<Object> getMatchValue() {
		return matchValue;
	}

	public void setMatchValue(List<Object> matchValue) {
		this.matchValue = matchValue;
	}

	public List<String> getGroupIndex() {
		return groupIndex;
	}

	public void setGroupIndex(List<String> groupIndex) {
		this.groupIndex = groupIndex;
	}

	public GroupGlobalMatch(Object value,String index){
		matchValue.add(value);
		groupIndex.add(index);
	}

	public void add(Object value, String index) {
		// TODO Auto-generated method stub
		matchValue.add(value);
		groupIndex.add(index);
	}
}
