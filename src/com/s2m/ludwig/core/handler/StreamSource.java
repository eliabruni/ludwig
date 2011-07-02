package com.s2m.ludwig.core.handler;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import com.s2m.ludwig.conf.OSSConfiguration;
import com.s2m.ludwig.stream.TwitterStreamConnection;


import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;





public class StreamSource implements Runnable {
	
	
	String topic;
	
	public StreamSource(String topic) {
		this.topic = topic;
	}
	
	
	LinkedBlockingQueue<String> messageQueue;

	static OSSConfiguration conf = OSSConfiguration.get();

	private static ProducerConfig createProducerConfig() {
		// TODO: Use OSSConfiguration

		Properties props = new Properties();
		props.put("serializer.class", conf.getStreamSource());
		props.put("zk.connect", "127.0.0.1:2181");

		return new ProducerConfig(props);
	}
	

	 class Dequeuer implements Runnable {
		private int id;
		LinkedBlockingQueue<String> messageQueue;
		
		// TODO: put in OSSConfiguration
		public static final int MESSAGE_SIZE = 10;


		public Dequeuer(int id, LinkedBlockingQueue<String> messageQueue) {
			this.id = id;
			this.messageQueue = messageQueue;

		}


		public  void run() {
			Properties props = new Properties();
			 props.put("serializer.class", conf.getStreamSource());
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
