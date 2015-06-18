package com.in.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLReader {

	public static void main(String[] args) throws Exception {
		
//		System.out.println(getPicID("41234002308"));
//		System.out.println(getGelTime("41234002308"));
//		System.out.println(getGelTime("363952"));
		 getBGID("http://itugo.com/api/getclickdata-1?id=543f5323344189d80a000c05&limit=3");
//		 getBGID("http://itugo.com/api/getclickdata-1?id=543f5323344189d80a000c05&limit=2");
//		 getBGID("http://itugo.com/api/getclickdata-1?id=543f5323344189d80a000c05&limit=3");
//		 getBGID("http://itugo.com/api/getclickdata-1?id=543f5323344189d80a000c05&limit=4");
//		 getBGID("http://itugo.com/api/getclickdata-1?id=543f5323344189d80a000c05&limit=5");
		 
	
	}
	
	public static String getBGID(String surl) throws IOException {
		URL url = new URL(surl);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				url.openStream()));

		String inputLine = null;
		
		while ((inputLine = in.readLine()) != null) {
			 System.out.println(inputLine);
		}

		in.close();
		return inputLine;
	}

	public static String getPicID(String tdid) throws IOException {
		URL url = new URL("http://www.itugo.com/?r=api/getgoodbyitem&item_id="
				+ tdid);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				url.openStream()));

		String inputLine = null;
		
		while ((inputLine = in.readLine()) != null) {
			 System.out.println(inputLine);
			Pattern p1 = Pattern.compile("\"pic_id\":\"([0-9]+)\"");
			Matcher m1 = p1.matcher(inputLine);

			if (m1.find()) {
				return m1.group(1);
			}
		}

		in.close();
		return inputLine;
	}
	public static String getGelTime(String tdid) throws IOException {
		URL url = new URL("http://itugo.com/api/getpicdetail?id="
				+ tdid);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				url.openStream()));

		String inputLine = null;
		 System.out.println(inputLine);
		while ((inputLine = in.readLine()) != null) {
			Pattern p1 = Pattern.compile("\"created_at\":\"([0-9]+)\"");
			Matcher m1 = p1.matcher(inputLine);

			if (m1.find()) {
				return m1.group(1);
			}
		}

		in.close();
		return inputLine;
	}

}