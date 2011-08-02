package com.s2m.ludwig.core.cooccur.collector;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.ImmutableMap;

import com.s2m.ludwig.conf.LudwigConfiguration;
import com.s2m.ludwig.persister.cooccur.lsh.LSHSink;
import com.s2m.ludwig.persister.cooccur.sink.CooccursSink;
import com.s2m.ludwig.util.lsh.LSH;


public class LSHCooccurCollector extends Thread {
	// TODO:
	// 0. Introduce new Cooccurs and Coccur classes like in Agent.
	// 1. A way to decide how to flush cooccurs to persister (based on the space cooccurs is consuming).
	// 2. We need (together with cooccurs) to flush sumArrays to LSHTable.
	private final Logger LOG = LoggerFactory.getLogger(CooccurCollector.class);

	/**
	 * Retain the number of same type of collectors.
	 */
	private int NUMBER_OF_SAME_COLLECTORS;

	/**
	 * Retain the number of different type of collectors.
	 */
	private int NUMBER_OF_DIFFERENT_COLLECTORS;

	/**
	 * The consumer of the stream.
	 */
	protected final ConsumerConnector consumer;

	/**
	 * The topic of that consumes this collector.
	 */
	private String topic;
	
	/**
	 * The HBase table where to flush the cooccurrences.
	 */
	private CooccursSink cooccursSink;
	
	/**
	 * Locality Sensitive Hash to approximate cosines.
	 */
	private LSH lsh;
	
	/**
	 * The signatures to compute cosine similarities.
	 */
	private LongObjectOpenHashMap<byte[]> signatures;
	
	/**
	 * 
	 */
	private static LongObjectOpenHashMap<LongIntOpenHashMap> cooccurs;
	
	static LudwigConfiguration conf = LudwigConfiguration.get();
	

	/**********************************************************************************
	 * Constructors
	 **********************************************************************************/
	
	public LSHCooccurCollector(String topic) throws IOException {
		this(conf.getNumberOfSameCollectors(), conf.getNumberOfDifferentCollectors());
		this.topic = topic;
		this.signatures = new LongObjectOpenHashMap<byte[]>();
	}

	public LSHCooccurCollector(int NUMBER_OF_SAME_COLLECTORS, int NUMBER_OF_DIFFERENT_COLLECTORS) throws IOException {
		this.NUMBER_OF_SAME_COLLECTORS = NUMBER_OF_SAME_COLLECTORS;
		this.NUMBER_OF_DIFFERENT_COLLECTORS = NUMBER_OF_DIFFERENT_COLLECTORS;
		cooccurs = new LongObjectOpenHashMap<LongIntOpenHashMap>();
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		cooccursSink = new CooccursSink();
		lsh = new LSH();
	}
	
	
	/**********************************************************************************
	 * Configuration methods
	 **********************************************************************************/

	private static ConsumerConfig createConsumerConfig() {
		// TODO: Use OSSConfiguration and check in http://sna-projects.com/kafka/configuration.php
		// to set the right config

		Properties props = new Properties();
		props.put("zk.connect", "localhost:2181");
		props.put("groupid", "tweets_group");
		props.put("zk.sessiontimeout.ms", "400");
		props.put("zk.connectiontimeout.ms", "1000000");
		props.put("zk.synctime.ms", "200");
		props.put("autocommit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}
	

	/**********************************************************************************
	 * Main methods
	 **********************************************************************************/

	public void run() {
		
		// TODO: comment here + maybe change the way to access to conf
		Map<String, List<KafkaMessageStream>> MessageStreams = 
			consumer.createMessageStreams(ImmutableMap.of(topic, NUMBER_OF_DIFFERENT_COLLECTORS));
		List<KafkaMessageStream> streams = MessageStreams.get(topic);

		// TODO: comment here + maybe change the way to access to conf
		ExecutorService executors = Executors.newFixedThreadPool(NUMBER_OF_SAME_COLLECTORS);

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

	
	/**********************************************************************************
	 * CooccurCollector helper functions
	 * @throws IOException 
	 **********************************************************************************/

	/**
	 * Flush all the cooccurs to HBase and create+flush all the related sumArrays.
	 */
	private void flushCooccurs() throws IOException {
		final boolean outerStates[] = cooccurs.allocated;
		final long[] outerKeys = cooccurs.keys;
		
		for (int k = 0; k < outerStates.length; k++) {
			if(outerStates[k]) {
				long term = outerKeys[k];
				LongIntOpenHashMap cooccur = cooccurs.get(term);
				cooccursSink.flushWord(term, cooccur);
				lsh.buildSumArray(term, cooccur);
			}
		}
		
		lsh.flushSumrrays();
		cooccurs = new LongObjectOpenHashMap<LongIntOpenHashMap>();
	}
	
	
	@SuppressWarnings("null")
	private void updateWordsCooccurs(byte[] body) {
		ByteBuffer buffer = ByteBuffer.wrap(body);

		int SIZEOF_LONG = 8;
		int SIZEOF_INT = 4;
		int SIZEOF_BYTE = 1;
		int pointer = 0;

		long term = -1;
		LongIntOpenHashMap termCooccur = null;

		while (pointer < buffer.capacity()) {
			byte magic = buffer.get(pointer);


			if (magic == Byte.MIN_VALUE) {
				pointer += SIZEOF_BYTE;
				term = buffer.getLong(pointer);
				pointer += SIZEOF_LONG;

				long otherTerm = buffer.getLong(pointer);
				pointer += SIZEOF_LONG;
				int newCount = buffer.getInt(pointer);
				pointer += SIZEOF_INT;


				if (cooccurs.containsKey(term)) {
					termCooccur = cooccurs.get(term);

					innerScroll(term, termCooccur, otherTerm, newCount);
				} 

				else {
					termCooccur = new LongIntOpenHashMap();
					termCooccur.put(otherTerm, newCount);
					cooccurs.put(term, termCooccur);
				}

			} 

			else {
				long otherTerm = buffer.getLong(pointer);
				pointer += SIZEOF_LONG;
				int newCount = buffer.getInt(pointer);
				pointer += SIZEOF_INT;

				innerScroll(term, termCooccur, otherTerm, newCount);
			}
		}
	}

	private void innerScroll(long term, LongIntOpenHashMap termCooccur,
			long otherTerm, int newCount) {
		if (termCooccur.containsKey(otherTerm)) {
			int oldCount = termCooccur.get(otherTerm);
			termCooccur.put(otherTerm, oldCount + newCount);
			cooccurs.put(term, termCooccur);

			// DEBUG
			//int increment = newCount + oldCount;
			//String message = "incrementing: " + term + ", oldCount: " + oldCount + ", incremented to: " + increment;
			//pw2.println(message);
			//pw2.flush();
			// DEBUG

		} 

		else {
			termCooccur.put(otherTerm, newCount);
			cooccurs.put(term, termCooccur);
		}
	}
	
}

