package com.s2m.ludwig.twitter;

import com.s2m.ludwig.core.agent.CooccurAgent;
import com.s2m.ludwig.core.collector.CooccurCollector;
import com.s2m.ludwig.core.handler.StreamSource;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Documentation for this stream can be found here:
	    // http://apiwiki.twitter.com/Streaming-API-Documentation
		StreamSource twitterStreamSource = new StreamSource("tweet", "", new TwitterStreamConnection());
		twitterStreamSource.run();
		
		CooccurAgent twitterCooccurAgent = new CooccurAgent("tweet");
		twitterCooccurAgent.run();
		
		CooccurCollector twitterCoccurCollector = new CooccurCollector("tweet");
		twitterCoccurCollector.run();

	}

}
