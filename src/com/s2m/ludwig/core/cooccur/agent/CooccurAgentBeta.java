package com.s2m.ludwig.core.cooccur.agent;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.ImmutableMap;

import com.s2m.ludwig.conf.LudwigConfiguration;
import com.s2m.ludwig.core.cooccur.Cooccur;
import com.s2m.ludwig.core.cooccur.Cooccurs;
import com.s2m.ludwig.core.docs.Docs;
import com.s2m.ludwig.core.users.Users;
import com.s2m.ludwig.twitter.TwitterStatus;
import com.s2m.ludwig.twitter.TwitterUser;


public class CooccurAgentBeta extends Thread {
	// TODO: 
	// 1. Change the flush-to-broker condition based on space consuming.
	// 2. Create a class Cooccurs, similar to Users and introduce here.		
	// 3. A flowchart and/or pseudocode here, to explain all what CooccurAgent is doing.
	private final Logger LOG = LoggerFactory.getLogger(CooccurAgentBeta.class);

	/**
	 * This class retains all the info about the cooccurs.
	 */
	private Cooccurs cooccurs;
	
	/**
	 * An utility class to compute cooccurs.
	 */
	private Cooccur cooccur;
	
	/**
	 * This class retains all the info about the users.
	 */
	private Users users;

	/**
	 * This class retains all the info about the docs.
	 */
	private Docs docs;

	/**
	 * Number of TP for an agent. 
	 */
	private int NUMBER_OF_TP;

	/**
	 * This is the threshold after which a TP has to be emptied and its 
	 * cooccur sent to the broker.
	 */
	private int TP_THRESHOLD;

	/**
	 * Each partition has a counter to determine when to send its cooccur to the broker.
	 */
	private int[] TPCounters;

	/**
	 * The producers to forward cooccur to collectors. 
	 * A producer for each partition is created.
	 */
	private final Producer[] producers;

	/**
	 * The consumer of the stream.
	 */
	protected final ConsumerConnector consumer;

	/**
	 * The topic of that consumes this collector.
	 */
	private String topic;

	static LudwigConfiguration conf = LudwigConfiguration.get();
	

	/**********************************************************************************
	 * Constructors
	 **********************************************************************************/

	public CooccurAgentBeta(String topic) throws IOException {
		this(conf.getNumberOfTP(), conf.getTPThreshold());
		this.topic = topic;
	}

	public CooccurAgentBeta(int NUMBER_OF_TP, int TP_THRESHOLD) throws IOException {
		this.NUMBER_OF_TP = NUMBER_OF_TP;
		this.TP_THRESHOLD = TP_THRESHOLD;
		cooccurs = new Cooccurs();
		users = new Users();
		docs = new Docs();
		cooccur = new Cooccur(cooccurs, NUMBER_OF_TP, users, docs);
		TPCounters = new int[NUMBER_OF_TP];
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		producers = new Producer[NUMBER_OF_TP];
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

	private static ProducerConfig createProducerConfig() {
		// TODO: Use OSSConfiguration
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", "127.0.0.1:2181");

		return new ProducerConfig(props);
	}
	

	/**********************************************************************************
	 * Main methods
	 **********************************************************************************/
	
	/**
	 * Compute cooccur of tweets coming from TwitterStreamingSource.
	 */
	public void run() {

		LudwigConfiguration conf = LudwigConfiguration.get();

		// Initialize the list of collectors
		init();

		// create NUMBER_OF_AGENTS partitions of the stream for topic “tweet”, to allow NUMBER_OF_THREADS_PER_AGENT threads to consume
		Map<String, List<KafkaMessageStream>> TPMessageStreams = 
			consumer.createMessageStreams(ImmutableMap.of(topic, conf.getNumberOfAgents()));

		List<KafkaMessageStream> streams = TPMessageStreams.get(topic);

		// create list of NUMBER_OF_THREADS_PER_AGENT threads to consume from each of the partitions 
		ExecutorService executors = Executors.newFixedThreadPool(conf.getNumberOfThreadsPerAgent());

		// consume the messages in the threads
		for (final KafkaMessageStream stream: streams) {
			executors.submit(new Runnable() {

				public void run() {

					for(Message inMsg: stream) {

						JsonNode json = null;
						try {
							json = new ObjectMapper().readTree
							(new ByteArrayInputStream(inMsg.buffer().array()));
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						if(json.path("delete").isMissingNode()) {
							TwitterStatus twitterStatus = null;
							try {
								twitterStatus = new TwitterStatus(json);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							TwitterUser twitterUser = twitterStatus.user;
							String language = twitterUser.lang;

							if (language.equals("en")) {
								
								// We take the tweet message.
								String text = twitterStatus.text;
								// We take the user id.
								long userId = users.getUserId(twitterUser.name);
								// We take the doc id.
								long docId = docs.getDocId(twitterUser.url);
								
								// We add the current docId to the set of the current user's consumed docs.
								users.addDocToUser(userId, docId);
								// We add the current userId to the set of the current users that consumed this doc.
								docs.addUserToDoc(docId, userId);

								// Compute word co-occurrences of the document
								cooccur.insertCooccur(text, docId, userId, TPCounters);
								
								for (int TP = 0; TP < TPCounters.length; TP++) {
									
									// If the counter of this TP reaches the threshold, the cooccur for this TP are serialized and sent to
									// the broker. TPCounter is reinitialized. 
									if (TPCounters[TP] >= TP_THRESHOLD) {
										byte[] body = serialize(TP, TPCounters[TP], cooccurs.getCooccur(TP)); 
										ArrayList<byte[]> outMsg = new ArrayList<byte[]>();
										outMsg.add(body);
										sendToBroker(TP, outMsg);
										TPCounters[TP] = 0;
									}
								}

							}
						}
					}

				}
			});
		}
	}


	/**********************************************************************************
	 * CooccurAgent helper methods
	 **********************************************************************************/

	/**
	 * Initialize the list of collectors.
	 */
	protected void init() {
		// Initialize cooccurs and TPCounters
		for (int i = 0; i < NUMBER_OF_TP; i++) {
			cooccurs.addCooccurMap(new LongObjectOpenHashMap<LongIntOpenHashMap>());
		}
	}
	
	/**
	 * 
	 * Send cooccur relative to a particular TP to the broker. 
	 * The collector associated with this TP will consequently push the counts
	 * 
	 * @param itn TP
	 * @param ArrayList<byte[]> message
	 * 
	 */
	private void sendToBroker(int TP, ArrayList<byte[]> message) {
		if (producers[TP] == null) {
			// TODO: we got to use here TP as key in the producer properties for partitioning.
			producers[TP] = new Producer<String, byte[]>(createProducerConfig());
		}
		producers[TP].send(new ProducerData<String, byte[]>("tweet", new Integer(TP).toString(), message));
	}
	
	/**
	 * 
	 * Serialize the TP coccurs in order to be sent to the broker.
	 * 
	 * @return 
	 */
	private byte[] serialize(int TP, int TPCounter, LongObjectOpenHashMap<LongIntOpenHashMap> TPCooccur) {
		int SIZEOF_LONG = 8;
		int SIZEOF_INT = 4;
		int SIZEOF_BYTE = 1;

		int bufferSize = TPCooccur.keys().size() * SIZEOF_LONG + TPCounter 
		* SIZEOF_LONG + TPCounter * SIZEOF_INT + TPCooccur.keys().size() * SIZEOF_BYTE;

		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

		final boolean outerStates[] = TPCooccur.allocated;
		final long[] outerKeys = TPCooccur.keys;

		for (int i = 0; i < outerStates.length; i++) {
			if(outerStates[i]) {
				long term = outerKeys[i];
				buffer.put(Byte.MIN_VALUE);
				buffer.putLong(term);

				final boolean innerStates[] = TPCooccur.get(outerKeys[i]).allocated;
				final long[] innerKeys = TPCooccur.get(outerKeys[i]).keys;
				final int[] innerValues = TPCooccur.get(outerKeys[i]).values;

				for (int j = 0; j < innerStates.length; j++) {
					if (innerStates[j]) {
						long otherTerm = innerKeys[j];
						int count = innerValues[j];
						buffer.putLong(otherTerm);
						buffer.putInt(count);
					}
				}
			}
		}
		// Reset this TP
		// TODO: check if it's the right way
		cooccurs.resetTP(TP);
		buffer.flip();

		return buffer.array();
	}
	
}
