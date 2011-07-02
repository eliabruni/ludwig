package com.s2m.ludwig.twitter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.KafkaMessageStream;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.s2m.ludwig.conf.OSSConfiguration;
import com.s2m.ludwig.core.collector.StreamCollector;

public class TwitterStreamCollector extends StreamCollector {

	private final Logger LOG = LoggerFactory.getLogger(TwitterStreamCollector.class);


	public void run() {
		
		OSSConfiguration conf = OSSConfiguration.get();

		// TODO: comment here + maybe change the way to access to conf
		Map<String, List<KafkaMessageStream>> MessageStreams = 
			consumer.createMessageStreams(ImmutableMap.of("tweet", conf.getNumberOfDifferentCollectors()));
		List<KafkaMessageStream> streams = MessageStreams.get("tweet");

		// TODO: comment here + maybe change the way to access to conf
		ExecutorService executors = Executors.newFixedThreadPool(conf.getNumberOfSameCollectors());

		// consume the messages in the threads
		for (final KafkaMessageStream stream: streams) {
			executors.submit(new Runnable() {

				public void run() {

					for(Message message: stream) {
						updateWordsCooccurs(message.buffer().array());
					}
				}
			});
		}
		
	}

}
