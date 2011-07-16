package com.s2m.ludwig.core.stream;

import java.util.concurrent.LinkedBlockingQueue;

public abstract class StreamConnection implements Runnable {
	
	protected LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();

	public abstract void run();
	
	public abstract void connectAndRead() throws Exception;
	
	public LinkedBlockingQueue<String> getMessageQueue() {
		return messageQueue;
	}
	
	public String take() {
		try {
			return (messageQueue != null) ? messageQueue.take() : null;
		}
		catch(InterruptedException e) {
			return null;
		}
	}
	
}
