package com.s2m.ludwig.core.handler;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import com.s2m.ludwig.conf.OSSConfiguration;
import com.s2m.ludwig.core.stream.StreamConnection;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;


public class StreamSource implements Runnable {

	private String topic;
	private String streamSource;
	private StreamConnection stream;
	
	public StreamSource(String topic, String stremSource, StreamConnection stream) {
		this.topic = topic;
		this.streamSource = stremSource;
		this.stream = stream;
	}

	LinkedBlockingQueue<String> messageQueue;

	static OSSConfiguration conf = OSSConfiguration.get();

	private ProducerConfig createProducerConfig() {
		// TODO: Use OSSConfiguration

		Properties props = new Properties();
		props.put("serializer.class", streamSource);
		props.put("zk.connect", "127.0.0.1:2181");

		return new ProducerConfig(props);
	}

	class Dequeuer implements Runnable {
		private int id;
		LinkedBlockingQueue<String> messageQueue;

		// TODO: put in OSSConfiguration
		public final int MESSAGE_SIZE = conf.getMessageSize();


		public Dequeuer(int id, LinkedBlockingQueue<String> messageQueue) {
			this.id = id;
			this.messageQueue = messageQueue;
		}

		public  void run() {
			Properties props = new Properties();
			props.put("serializer.class", streamSource);
			props.put("zk.connect", "127.0.0.1:2181");
			Producer producer = new Producer(createProducerConfig());

			while (!Thread.interrupted()) {
				try {

					ArrayList<String> messages = new ArrayList<String>();

					for (int i = 0; i < MESSAGE_SIZE; i++) { 

						String message = messageQueue.take();
						messages.add(message);
					}

					// TODO:
					// Check if there's a smarter/faster way to serialize messages
					producer.send(new kafka.javaapi.producer.ProducerData<String, ArrayList<String>>(topic, messages));

				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					//Logger.getLogger("s4").error("Exception processing message", e);
				}
			}

			producer.close();
		}
	}

	public synchronized void open(){
		// TODO: check if it's the right way
		stream.run();
		messageQueue = stream.getMessageQueue();
	}

	@Override
	public void run() {

		open();

		for (int i = 0; i < 12; i++) {
			Dequeuer dequeuer = new Dequeuer(i, messageQueue);
			Thread t = new Thread(dequeuer);
			t.start();
		}

		(new Thread(this)).start();
	}
	
}
