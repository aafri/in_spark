package com.in.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.in.util.Consnt;

/**
 * xml配置中的<entry></entry> 如：<entry field_name="request"
 * match_type="contain"><![CDATA[/app/paster/leftcount]]></entry>
 * 
 * entry条件规则类，在不同的父标签下具有不同的属性值，但都是用于规则匹配。
 * 
 * 在match标签下entry的field_name有以下几种:
 * request,ip,timestamp,referer,hostname,httpstate,agent,all
 * 
 * match_type有contain,equal,regexp,uncontain,unregexp
 * match_type则为equal，match，combine3种
 * 
 * 在group标签下，entry的field_name主要是正则表达式中的组序号，如1，2，3
 * 
 * @author Tang Zeliang
 */
public class EntryUnit {

	Pattern pattern = null;
	Matcher matcher = null;

	public String fieldName;
	public String matchType;
	public String matchValue;

	List<String> groupValue = new ArrayList<String>();

	public EntryUnit(String fieldName, String matchType, String matchValue) {
		this.fieldName = fieldName;
		this.matchType = matchType;
		this.matchValue = matchValue;

		if (matchType.equals(Consnt.MATCHTYPE_IDENTICAL)) {
			// do nothing;
		} else if (matchType.equals(Consnt.MATCHTYPE_RX)) {
			pattern = Pattern.compile(matchValue);
		} else if (matchType.equals(Consnt.MATCHTYPE_SIMPLE)) {
			// nothing;
		} else if (matchType.equals(Consnt.MATCHTYPE_UNRX)) {
			pattern = Pattern.compile(matchValue);
		} else if (matchType.equals(Consnt.MATCHTYPE_UNCONTAIN)) {
			pattern = Pattern.compile(matchValue);
		}
	}

	public boolean matches(Map<String, String> fields) {
		if (fields.get(fieldName) != null) {
			if (matchType.equals(Consnt.MATCHTYPE_IDENTICAL)) {
				return fields.get(fieldName).equals(matchValue);
			} else if (matchType.equals(Consnt.MATCHTYPE_RX)) {
				matcher = pattern.matcher(fields.get(fieldName));
				return matcher.find();
			} else if (matchType.equals(Consnt.MATCHTYPE_SIMPLE)) {
				return fields.get(fieldName).contains(matchValue);
			} else if (matchType.equals(Consnt.MATCHTYPE_UNRX)) {
				matcher = pattern.matcher(fields.get(fieldName));
				return !matcher.find();
			} else if (matchType.equals(Consnt.MATCHTYPE_UNCONTAIN)) {
				return !fields.get(fieldName).contains(matchValue);
			}
		}
		return false;
	}

	/**
	 * 返回正则表达式的捕获组
	 * 
	 * @param fields
	 *            经正则表达式解析后的各字段Map对象
	 * @return 返回正则表达式的捕获组
	 */
	public List<String> getGroupList(Map<String, String> fields) {
		if (matchType.equals(Consnt.MATCHTYPE_RX)) {
			matcher = pattern.matcher(fields.get(fieldName));
			if (matcher.find()) {
				for (int i = 1; i <= matcher.groupCount(); i++) {
					groupValue.add(matcher.group(i));
				}
			}
		}
		return groupValue;
	}
}
