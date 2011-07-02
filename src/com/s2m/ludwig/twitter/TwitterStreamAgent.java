package com.s2m.ludwig.twitter;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.KafkaMessageStream;
import kafka.message.Message;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;
import com.s2m.ludwig.conf.OSSConfiguration;
import com.s2m.ludwig.core.agent.StreamAgent;





// TODO:
// 1. Use com.google.common.base.Preconditions;

public class TwitterStreamAgent extends StreamAgent {

	

	/**
	 * Compute cooccur of tweets coming from TwitterStreamingSource.
	 */
	public void run() {
		
		OSSConfiguration conf = OSSConfiguration.get();
		
		// Initialize the list of collectors
		init();

		// create NUMBER_OF_AGENTS partitions of the stream for topic “tweet”, to allow NUMBER_OF_THREADS_PER_AGENT threads to consume
		Map<String, List<KafkaMessageStream>> TPMessageStreams = 
			consumer.createMessageStreams(ImmutableMap.of("tweet", conf.getNumberOfAgents()));

		List<KafkaMessageStream> streams = TPMessageStreams.get("tweet");

		// create list of NUMBER_OF_THREADS_PER_AGENT threads to consume from each of the partitions 
		ExecutorService executors = Executors.newFixedThreadPool(conf.getNumberOfThreadsPerAgent());

		// consume the messages in the threads
		for (final KafkaMessageStream stream: streams) {
			executors.submit(new Runnable() {

				public void run() {

					for(Message message: stream) {

						JsonNode json = null;
						try {
							json = new ObjectMapper().readTree
							(new ByteArrayInputStream(message.buffer().array()));
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						if(json.path("delete").isMissingNode()) {
							Status status = null;
							try {
								status = new Status(json);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							User user = status.user;
							String language = user.lang;

							if (language.equals("en")) {

								String text = status.text;

								// Compute word co-occurrences of the document
								insertCooccur(text);

							}
						}
					}

				}
			});
		}

	}

}
