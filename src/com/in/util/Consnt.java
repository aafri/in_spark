package com.in.util;

import java.util.HashSet;
import java.util.Set;

public final class Consnt {

	private Consnt() {}

	public static final String MATCHCOMBINETYPE_AND = "and";

	public static final String MATCHCOMBINETYPE_OR = "or";

	public static final String ACTIONTYPE_POSITION = "position";

	public static final String ACTIONTYPE_EXTRACT = "extract";
	
	// 增加正则匹配
	public static final String MATCHTYPE_SIMPLE = "contain";

	public static final String MATCHTYPE_RX = "regexp";

	public static final String MATCHTYPE_UNRX = "unregexp";

	public static final String MATCHTYPE_IDENTICAL = "equal";

	public static final String MATCHTYPE_UNCONTAIN = "uncontain";

	public static final String LOGITEM_IP = "ip";
	
	public static final String LOGITEM_TIMESTAMP = "timestamp";

	public static final String LOGITEM_STATE = "httpstate";
	
	public static final String LOGITEM_SIZE = "size";

	public static final String LOGITEM_REFERER = "referer";

	public static final String LOGITEM_COOKIE = "cookie";
	
	public static final String LOGITEM_RESPONSE = "response";
	
	public static final String LOGITEM_AK = "ak";
	
	public static final String LOGITEM_TOKEN = "token";

	public static final String LOGITEM_HOSTNAME = "hostName";

	public static final String LOGITEM_REQ = "request";
	
	public static final String LOGITEM_AGENT = "agent";

	public static final String LOGITEM_URL = "url";

	public static final String LOGITEM_ALL = "all";

	public static final String SPLITTER_KEYVALUE = "#:#";

	public static final String SPLITTER_INNERKEY = "@:@";

	public static final String SPLITTER_RESULTFILE = " ";

	public static final String TABLENAME = "tableName";

	public static final String FIELDNAME = "fieldName";

	
	/* 新增用于Hadoop MapReduce相关变量 */
	public static final String STAT_XML_FILE = "stat.xml.file";
	public static final String FILTER_XML_FILE = "filter.xml.file";
	public static final String STAT_PRODUCT_NAME = "stat.product.name";
	public static final String STAT_IN_PATH = "stat.in.path";
	public static final String STAT_OUT_PATH = "stat.out.path";
	public static final String STAT_DATE_STR = "stat.date.str";
	public static final String STAT_LOG_TYPE = "stat.log.type";
	public static final String STAT_INTERVAL_TYPE = "stat.interval.type";

	
	/**
	 * 过滤刷屏用户，刷屏网站黑名单
	 */
	public static Set<String> blackList = new HashSet<String>();
	static {
		blackList.add("www.aaa.com");
		blackList.add("www.bbb.cn");
	}

}
