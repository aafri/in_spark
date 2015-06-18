package com.in.report;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class URLReporter implements java.io.Serializable {
	private static final long serialVersionUID = 211098761523845855L;

	public static Logger LOG = Logger.getLogger(URLReporter.class);

	public URLReporter() {

	}

	public class PostRequest implements Runnable {
		private String connectURL = null;
		private String message = null;
		private JSONObject outData = null;

		public PostRequest(String url) {
			this.connectURL = url;
		}

		public PostRequest(String url, String content) {
			this.connectURL = url;
			this.message = content;
		}

		public PostRequest(String url, JSONObject outData) {
			this.connectURL = url;
			this.outData = outData;
		}

		public void run() {

			HttpURLConnection conn = null;
			URL connURL = null;
			try {
				connURL = new URL(connectURL);
				// connect to server
				conn = (HttpURLConnection) connURL.openConnection();

				// set method
				conn.setRequestMethod("POST");
				conn.setDoInput(true);
				conn.setDoOutput(true);

				LOG.info("conentctURL:" + connectURL);

				// write out the message
				DataOutputStream out = new DataOutputStream(
						conn.getOutputStream());

				out.writeBytes(outData.toString());
				out.flush();
				out.close();
				System.out.println(conn.getResponseCode());
				// read the input
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(conn.getInputStream(), "utf-8"));
				reader.close();
				conn.disconnect();
				LOG.info("Post success , url:" + connectURL);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				LOG.info(e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				LOG.info(e.getMessage());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void runJson() {

		}

	}

	/*
	 * 
	 */
	public void doPostConnect(String connectURL, JSONObject outData) {
		// Do it with thread pool to avoid block thread issue
		PostRequest pr = new PostRequest(connectURL, outData);
		pr.run();
	}

	public static void main(String[] args) throws IOException, JSONException {
		// test();
		// 99863 8 0 8 22 0.36363636363636365
		FileReader fr = new FileReader(args[0]);
		long total = Long.parseLong(args[1]);
		// FileReader fr = new FileReader("f:\\20141020");
		BufferedReader bufr = new BufferedReader(fr);

		String line = null;
		String[] strs = null;

		int j = 0;
		// long total = 2694680;
		int i = 0;

		JSONArray gResTable = new JSONArray();
		JSONObject outData = new JSONObject();
		outData.put("ResTable", gResTable);

		while ((line = bufr.readLine()) != null) {
			
			strs = line.trim().split("\t");
			if (strs.length == 3) {
				if (j < 1000) {
					test(gResTable, strs[0], strs[1], strs[2]);
					j++;
					i++;
				} else {
//					System.out.println(outData.toString());
					URLJsonReporter report = new URLJsonReporter();
					report.doPostConnect("http://itugo.com/?r=api/picscore"
					// "http://www.local-itugo.com/?r=api/picscore"
							, outData);

					j = 0;
					i++;
					gResTable = new JSONArray();
					outData = new JSONObject();
					outData.put("ResTable", gResTable);
					test(gResTable, strs[0], strs[1], strs[2]);
				}

				if (i == total) {
//					System.out.println(outData.toString());
					URLJsonReporter report = new URLJsonReporter();
					report.doPostConnect("http://itugo.com/?r=api/picscore"
					// "http://www.local-itugo.com/?r=api/picscore"
							, outData);
				}
			}

		}
		bufr.readLine();
		bufr.close();
	}

	public static void test(JSONArray gResTable, String id, String id2,
			String score) throws JSONException {

		JSONObject node = new JSONObject();
		node.put("pid", id);
		node.put("pic_id", id2);
		node.put("origin_score", score);
		gResTable.put(node);

		// System.out.println(outData.toString());
	}
}
