package com.s2m.ludwig.twitter;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;



import stream.TwitterStreamingConnection;


public class TwitterStreamSource implements Runnable {

	LinkedBlockingQueue<String> messageQueue;



	class Dequeuer implements Runnable {
		private int id;
		LinkedBlockingQueue<String> messageQueue;
		
		// TODO: put in OSSConfiguration
		public static final int MESSAGE_SIZE = 10;


		public Dequeuer(int id, LinkedBlockingQueue<String> messageQueue) {
			this.id = id;
			this.messageQueue = messageQueue;

		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void run() {

			Properties props = new Properties();
			props.put("serializer.class", "tweets.twitterStreamingSource");
			props.put("zk.connect", "127.0.0.1:2181");
			ProducerConfig config = new ProducerConfig(props);
			Producer producer = new Producer(config);

			while (!Thread.interrupted()) {
				try {

					ArrayList<String> messages = new ArrayList<String>();
					
					for (int i = 0; i < MESSAGE_SIZE; i++) {

						String message = messageQueue.take();
						messages.add(message);
					}
					
					
					// TODO:
					// Check if there's a smarter/faster way to serialize messages
					producer.send(new kafka.javaapi.producer.ProducerData<String, ArrayList<String>>("tweet", messages));
					

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
		TwitterStreamConnection stream = new TwitterStreamConnection();
		try {
			stream.connectAndRead();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
