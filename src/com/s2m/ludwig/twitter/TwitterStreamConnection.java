package com.s2m.ludwig.twitter;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.util.EncodingUtil;

import com.s2m.ludwig.conf.LudwigConfiguration;
import com.s2m.ludwig.core.stream.StreamConnection;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  Twitter's streaming API requires a valid username and password.
 *  The requested url gets a stream of sample tweets in json format 
 *  (xml is available as well). There is one entry per line.
 *  
 *  Remarks
 *  -------
 *  Documentation for this stream can be found here:
 *  http://apiwiki.twitter.com/Streaming-API-Documentation
 *
 *************************************************************************/
public class TwitterStreamConnection extends StreamConnection {

	private String urlString;
	private String userid;
	private String password;
	private long maxBackoffTime = 30 * 1000; // 5 seconds
	private long messageCount = 0;
	private long blankCount = 0;
	private String streamName;

	static LudwigConfiguration conf = LudwigConfiguration.get();

	public TwitterStreamConnection() {
		this(conf.getTwitterURL(), conf.getTwitterName(), conf.getTwitterPW());
	}

	public TwitterStreamConnection(String urlString, String userid, String password) {
		this.urlString = urlString;
		this.userid = userid;
		this.password = password;
	}

	public void setMaxBackoffTime(long maxBackoffTime) {
		this.maxBackoffTime = maxBackoffTime;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public void run() {
		long backoffTime = 1000;
		while (!Thread.interrupted()) {
			try {
				connectAndRead();
			} catch (Exception e) {
				//Logger.getLogger("s4").error("Exception reading feed", e);
				try {
					Thread.sleep(backoffTime);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
				backoffTime = backoffTime * 2;
				if (backoffTime > maxBackoffTime) {
					backoffTime = maxBackoffTime;
				}
			}
		}
	}

	public void connectAndRead() throws Exception {
		URL url = new URL(urlString);

		URLConnection connection = url.openConnection();
		String userPassword = userid + ":" + password;
		String encoded = EncodingUtil.getAsciiString(Base64.encodeBase64(EncodingUtil.getAsciiBytes(userPassword)));
		connection.setRequestProperty("Authorization", "Basic " + encoded);
		connection.connect();

		InputStream is = connection.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);

		String inputLine = null;
		while ((inputLine = br.readLine()) != null) {
			if (inputLine.trim().length() == 0) {
				blankCount++;
				continue;
			}
			messageCount++;
			messageQueue.add(inputLine);
		}
	}

}
