package com.in.log;

import java.util.regex.Pattern;


public class LogRegex {

	// 113.200.85.168 - - [02/Sep/2014:00:00:06 +0800]
	// "GET /?r=mobile/site/ad HTTP/1.1"
	// 200 108 "http://m.itugo.com/search"
	// "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/11B651 Weibo (iPhone5,2__weibo__4.2.0__iphone__os7.0.6)"
	// m.itugo.com eh32dmru93atoijf84u1afj9f1 - - - -
	// 1315f47b15a7750099a03bcfc088f136 0.018

	public final static Pattern ACCESSLOG = Pattern.compile(
	// IP
	// "^(\\S+)" + "\\s" + "\\S+" + "\\s" + "\\S+" + "\\s"
			// +
			// // 时间戳
			// "\\["
			// +
			"^(\\S+)"
					+ "\\s"
					+
					// 时区
					"[^]]+?\\]"
					+ "\\s"
					+
					// Request
					"\"(\\S+\\s\\S+\\sHTTP/1\\.[01])\""
					+
					// 状态码״̬
					"\\s"
					+ "(\\d+)"
					+ "\\s"
					+
					// 字节数
					"(\\d+|-)"
					+ "\\s"
					// Refer
					+ "\"([^\"]*)\""
					+ "\\s"
					+
					// agent
					"\"([^\"]*)\""
					+ "\\s"
					+
					// Domain
					"(\\S+)"
					+
					// other
					"\\s" + "(\\S+)" + "\\s" + "(\\S+)" + "\\s" + "(\\S+)"
					+ "\\s" + "(\\S+)" + "\\s" + "(\\S+)" + "\\s" + "(\\S+)"
					+ "\\s" + "(\\S+)"
			// )
			);

	public final static Pattern ACCESSLOGTOTAL = Pattern.compile(
	// IP
			"^(\\S+)" + "\\s" + "\\S+" + "\\s"
					+ "\\S+"
					+ "\\s"
					+
					// 时间戳
					"\\[(\\S+)"
					+ "\\s"
					+
					// 时区
					"[^]]+?\\]"
					+ "\\s"
					+
					// Request
					"\"(\\S+\\s\\S+\\sHTTP/1\\.[01])\""
					+
					// 状态码״̬
					"\\s"
					+ "(\\d+)"
					+ "\\s"
					+
					// 字节数
					"(\\d+|-)"
					+ "\\s"
					// Refer
					+ "\"([^\"]*)\""
					+ "\\s"
					+
					// agent
					"\"([^\"]*)\""
					+ "\\s"
					+
					// Domain
					"(\\S+)"
					+
					// other
					"\\s" + "(\\S+)" + "\\s" + "(\\S+)" + "\\s" + "(\\S+)"
					+ "\\s" + "(\\S+)" + "\\s" + "(\\S+)" + "\\s" + "(\\S+)"
					+ "\\s" + "(\\S+)");

	public final static Pattern APACHELOG = Pattern.compile(
	// IP
			"^(\\S+)" + "\\s" + "\\S+" + "\\s" + "\\S+" + "\\s" +
			// 时间戳
					"\\[(\\S+)" + "\\s" +
					// 时区
					"[^]]+?\\]" + "\\s" +
					// Request
					"\"(\\S+\\s\\S+\\sHTTP/1\\.[01])\"" +
					// 状态码
					"\\s" + "(\\d+)" + "\\s" +
					// 字节数
					"(\\d+|-)" + "\\s" +
					// Refer
					"\"([^\"]*)\"" + "\\s" +
					// Usually is X
					"\"[^\"]+?\"" + "\\s" +
					// Domain
					"\"([^\"]+?)\"" + "\\s" +
					// Session id
					"\"([^\"]+?)\"" +
					// 响应时间
					"(?:\\s(\\S+))?" +
					// Agent
					"(?:\\s\"([^\"]+?)\")?");


}
