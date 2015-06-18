package main.com.in.report;

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

public class URLJsonReporter implements java.io.Serializable {

	private static final long serialVersionUID = -2110987615238545855L;

	public static Logger LOG = Logger.getLogger(URLJsonReporter.class);
	public static String url="http://itugo.com/api/addsolrpic?";
	
	public URLJsonReporter() {

	}

	public class PostRequest implements Runnable {
		
		private String connectURL = null;
		private JSONObject outData = null;

		public PostRequest(String url) {
			this.connectURL = url;
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
				System.out.println("Response Code:"+conn.getResponseCode());
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

	public static void putJson(JSONArray gResTable, String id, String click,
			String go, String score, String exposure) throws JSONException {
		// &id=102786&click=9&go=0&score=0.1875&exposure=48

		JSONObject node = new JSONObject();
		node.put("id", id);
		node.put("click", click);
		node.put("go", go);
		node.put("score", score);
		node.put("exposure", exposure);
		gResTable.put(node);

	}

	// http://www.local-itugo.com/api/addsolrpic?id=123&click=54&go=5&score=2&exposure=5

	public static void main(String[] args) throws IOException, JSONException {
		// test();
		// 99863 8 0 8 22 0.36363636363636365
		FileReader fr = new FileReader(args[0]);
		long total = Long.parseLong(args[1]);
		int size=Integer.parseInt(args[2]);
		// FileReader fr = new FileReader("f:\\201410171623");
		BufferedReader bufr = new BufferedReader(fr);

		String line = null;
		String[] strs = null;

		int j = 0;
		int i = 0;

		JSONArray gResTable = new JSONArray();
		JSONObject outData = new JSONObject();
		outData.put("ResTable", gResTable);

		while ((line = bufr.readLine()) != null) {
			strs = line.trim().split("\t");
			if (strs.length == 6) {
				if (j < size) {
					putJson(gResTable, strs[0], strs[1], strs[2], strs[5],
							strs[4]);
					j++;
					i++;
				} else {
					URLJsonReporter report = new URLJsonReporter();
					report.doPostConnect(url,
							outData);
					j = 0;
					i++;
					gResTable = new JSONArray();
					outData = new JSONObject();
					outData.put("ResTable", gResTable);
					putJson(gResTable, strs[0], strs[1], strs[2], strs[5],
							strs[4]);
				}

				if (i == total) {
					URLJsonReporter report = new URLJsonReporter();
					report.doPostConnect(url,
							outData);
				}
			}

		}
		bufr.readLine();
		bufr.close();
	}

}
