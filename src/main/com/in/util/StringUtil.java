package main.com.in.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	
	public static Map<String,String> getParams(String line){
		Map<String,String> params = new HashMap<String,String>();
		Pattern p = Pattern.compile("([^&=]*)=([^&=]*)");
		Matcher m = p.matcher(line);
		while(m.find()){
			params.put(m.group(1), m.group(2));
		}
		return params;
	}
	
	/**
	 * 将null对象转换成空字符
	 * @param obj
	 * @return
	 */
	public static String getString(String obj){
		return obj == null ? "" : obj;
	}
	
	/**
	 * 使用String类的indexOf方法来split整个字符串，避免使用正则表达
	 * @param src String
	 * @param a	String
	 * 字符串字面量，非正则表达
	 * @param limit
	 * @return
	 */
	public static String[] split(String src, String a, int limit) {
		List<String> list = new ArrayList<String>();
		int index = src.indexOf(a);
		int i = 0;
		int count = 0;
		while (index != -1) {
			list.add(src.substring(i, index));
			i = index + a.length();
			index = src.indexOf(a, i);
			count++;
			if(count >= limit -1){
				break;
			}
		}
		if (i < src.length()) {
			list.add(src.substring(i));
			count++;
		}

		String[] result = new String[count];
		return list.subList(0, count).toArray(result);
	}

	/**
	 * 使用String类的indexOf方法来split整个字符串，避免使用正则表达
	 * @param src String
	 * @param a	String
	 * 字符串字面量，非正则表达
	 * @return
	 */
	public static String[] split(String src, String a) {
		List<String> list = new ArrayList<String>();
		int index = src.indexOf(a);
		int i = 0;
		int count = 0;
		while (index != -1) {
			list.add(src.substring(i, index));
			i = index + a.length();
			index = src.indexOf(a, i);
			count++;
		}
		if (i < src.length()) {
			list.add(src.substring(i));
			count++;
		}

		String[] result = new String[count];
		return list.subList(0, count).toArray(result);
	}
	
}
