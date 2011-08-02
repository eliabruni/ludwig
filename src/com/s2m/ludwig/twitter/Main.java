package com.s2m.ludwig.twitter;

import java.io.IOException;

import com.s2m.ludwig.core.cooccur.agent.CooccurAgent;
import com.s2m.ludwig.core.cooccur.collector.CooccurCollector;
import com.s2m.ludwig.core.handler.StreamSource;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
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
