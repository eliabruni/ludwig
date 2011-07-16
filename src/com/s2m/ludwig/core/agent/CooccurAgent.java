package com.s2m.ludwig.core.agent;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.google.common.collect.ImmutableMap;

import com.s2m.ludwig.conf.OSSConfiguration;
import com.s2m.ludwig.persister.hdictionary.MemCachedHBaseDictionary;
import com.s2m.ludwig.twitter.Status;
import com.s2m.ludwig.twitter.User;
import com.s2m.ludwig.util.FileHandler;


// TODO:
// 1. Use com.google.common.base.Preconditions;
public class CooccurAgent extends Thread {
	// TODO: decide once for all which logger to use ans use it
	private final Logger LOG = LoggerFactory.getLogger(CooccurAgent.class);

	// Retain String -> long.
	private ObjectLongOpenHashMap<String> stringToLongDic = new ObjectLongOpenHashMap<String>();
	// TODO: change with HBaseDictionary

	// Retain long -> String .
	private LongObjectOpenHashMap<String> longToStringDic = new LongObjectOpenHashMap<String>();
	// TODO: change with HBaseDictionary

	// Counter for dictionary unique id .
	private long nextTerm = 0;
	// TODO: change with HBaseDictionary

	/**
	 * Retain all the cooccur counts.
	 */
	private ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>> cooccurs;

	/**
	 * The size of cooccur window
	 */
	private int WINDOW_SIZE;

	/**
	 * The path were to find the stopword list.
	 */
	private String STOPWORD_PATH;

	/**
	 * Number of TP for an agent 
	 */
	private int NUMBER_OF_TP;

	/**
	 * This is the threshold after which a TP has to be emptied and its 
	 * cooccur sent to the broker
	 */
	private int TP_THRESHOLD;

	/**
	 * Each partition has a counter to determine when to send its cooccur to the broker.
	 */
	private int[] partitionCounter;

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

	static OSSConfiguration conf = OSSConfiguration.get();


	public CooccurAgent(String topic) {
		this(conf.getWindowSize(), conf.getStopwordsPath(), conf.getNumberOfTP(), conf.getTPThreshold());
		this.topic = topic;
	}

	public CooccurAgent(int WINDOW_SIZE, String STOPWORD_PATH, int NUMBER_OF_TP, int TP_THRESHOLD) {
		this.WINDOW_SIZE = WINDOW_SIZE;
		this.STOPWORD_PATH = STOPWORD_PATH;

		this.NUMBER_OF_TP = NUMBER_OF_TP;
		this.TP_THRESHOLD = TP_THRESHOLD;

		cooccurs = new ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>>();
		partitionCounter = new int[NUMBER_OF_TP];

		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());


		// Not here anymore, the producer has to be created with configuration related to
		// the word partitioning.
		producers = new Producer[NUMBER_OF_TP];
	}

	/**
	 * Initialize the list of collectors.
	 */
	protected void init() {
		// Initialize cooccurs and TPCounters
		for (int i = 0; i < NUMBER_OF_TP; i++) {
			cooccurs.add(new LongObjectOpenHashMap<LongIntOpenHashMap>());
			partitionCounter[i] = 0;
		}
	}

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
		props.put("serializer.class", conf.getStreamSource());
		props.put("zk.connect", "127.0.0.1:2181");

		return new ProducerConfig(props);
	}


	/**
	 * Compute cooccur of tweets coming from TwitterStreamingSource.
	 */
	public void run() {
		
		OSSConfiguration conf = OSSConfiguration.get();
		
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

	/**
	 * 
	 * Send cooccur relative to a particular TP to the broker. 
	 * The collector associated with this TP will consequently push the counts
	 * 
	 * @param itn TP
	 * @param ArrayList<byte[]> message
	 * 
	 */
	public void sendToBroker(int TP, ArrayList<byte[]> message) {
		if (producers[TP] == null) {
			// TODO: we got to use here TP as key in the producer properties for partitioning.
			producers[TP] = new Producer<String, byte[]>(createProducerConfig());
		}

		producers[TP].send(new ProducerData<String, byte[]>("tweet", new Integer(TP).toString(), message));
	}

	/**********************************************************************************
	 * StreamAgent helper functions
	 **********************************************************************************/

	/**
	 *  Preprocess, convert terms to longs and populate termsCooccurs with 
	 *  the cooccur counts of the given document.
	 */
	protected void insertCooccur(String text) {
		Set<String> stopWords = null; 

		try {
			// TODO:
			// Put in a config file and use preconditions
			stopWords = FileHandler.readFileAsSet(STOPWORD_PATH);
		} 

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Reader reader = new StringReader(text);
		TokenStream stream = new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_30, 
				stopWords).tokenStream("content", reader);

		
		// TODO: manage the case of an empty document
		long[] terms = null;
		try {
			terms = convert(stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < terms.length; i++) {
			long term = terms[i];

			// We split the last term % NUMBER_OF_TP digits of term, 
			// to determine the actual TP.
			int TP = (int) (term % NUMBER_OF_TP);

			// Get the TPCooccur for this TP
			LongObjectOpenHashMap<LongIntOpenHashMap> TPCoccour = cooccurs.get(TP);

			// Get the counter for this TP
			int TPCounter = partitionCounter[TP];


			if (TPCoccour.containsKey(term)) {
				LongIntOpenHashMap map = TPCoccour.get(term);
				// Return the counter updated from the next sift window
				TPCounter = siftWindow(terms, TPCoccour, i, term, map, TPCounter);
				// TODO: It's a bad way, find a more elegant
				partitionCounter[TP] = TPCounter;
			} 

			else {
				LongIntOpenHashMap map = new LongIntOpenHashMap();
				// Return the counter updated from the next sift window
				TPCounter = siftWindow(terms, TPCoccour, i, term, map, TPCounter);
				partitionCounter[TP] = TPCounter;
			}

			// If the counter of this TP reaches the threshold,
			// the cooccur for this TP are serialized and sent to
			// the broker. All is reinitialized. 
			// TODO: is right to put a check here, or better a independent thread that
			//       every some time check. Even better, we should put here to different 
			//       thresholds: time and count.  
			if (TPCounter >= TP_THRESHOLD) {
				// TODO: We have to send a list of TPs' cooccur, whose size will be decided
				//       by a certain threshold. Once reached we send the list of messages to the broker
				byte[] body = serialize(TP, TPCounter, cooccurs.get(TP)); 
				ArrayList<byte[]> message = new ArrayList<byte[]>();
				message.add(body);
				
				sendToBroker(TP, message);
				partitionCounter[TP] = 0;
			}
		}
	}

	/**
	 * 
	 * Count the coooccur for the given window
	 * 
	 * @param terms
	 * @param window
	 * @param i
	 * @param term
	 * @param map
	 */
	private int siftWindow(long[] terms, LongObjectOpenHashMap<LongIntOpenHashMap> termsCooccur, 
			int i, long term, LongIntOpenHashMap map, int TPCounter) {

		for (int j = i - WINDOW_SIZE; j < i + WINDOW_SIZE + 1; j++) {
			if (j == i || j < 0)
				continue;

			if (j >= terms.length)
				break;

			if (map.containsKey(terms[j])) {
				map.put(terms[j], map.get(terms[j]) + 1);
				termsCooccur.put(term, map);
			} 

			else {
				map.put(terms[j], 1);
				termsCooccur.put(term, map);
				TPCounter++;
			}
		}

		return TPCounter;
	}
	
	/**
	 * 
	 * Map String --> long, through a HBasedictionary and a cache.
	 * @throws IOException 
	 * 
	 */
	private long[] convert(TokenStream stream) throws IOException {
		
		TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
		LongArrayList terms = new LongArrayList();
		MemCachedHBaseDictionary dic = new MemCachedHBaseDictionary();
		
		try {
			while (stream.incrementToken()) {
				String stringTerm = termAtt.term();
				terms.add(dic.convert(stringTerm));
			}
		} 

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return terms.toArray();
	}

	/**
	 * A in-memory String -> long mapping
	 * 
	 */
	private long[] convertInMemory(TokenStream stream) {
		TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
		LongArrayList terms = new LongArrayList();
		try {
			while (stream.incrementToken()) {
				String stringTerm = termAtt.term();
				if (stringToLongDic.containsKey(stringTerm)) {
					terms.add(stringToLongDic.get(stringTerm));
				} 

				else {
					terms.add(nextTerm);
					stringToLongDic.put(stringTerm, nextTerm);
					longToStringDic.put(nextTerm, stringTerm);
					nextTerm++;
				}
			}
		} 

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return terms.toArray();
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
		cooccurs.remove(TP);
		cooccurs.insert(TP, new LongObjectOpenHashMap<LongIntOpenHashMap>());

		buffer.flip();

		return buffer.array();
	}
}
