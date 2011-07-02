package com.s2m.ludwig.core.test;

import java.util.Date;
import java.util.GregorianCalendar;

public class Timer {

	private  long startTime;
	private  long relativeAnchorTime;
	private  long relativeTime ;
	private boolean initialTimeStatus = false;
	
	/**
	 * 
	 * @param label
	 */
	public void printTime(String label) {
		long anchor = new Date().getTime();
		if(!initialTimeStatus) {
			startTime = anchor;
			initialTimeStatus = true;
			System.out.println("  ->> " + label + ": " + new GregorianCalendar().getTime());
			System.out.println();
		}
		else {
			long absoluTime = anchor-startTime;
			if(relativeAnchorTime == 0) {
				System.out.println(label);
				System.out.println("  --> Time from START: " + absoluTime);
				System.out.println();
				relativeAnchorTime = anchor;
			}
				
			else {
				relativeTime = anchor - relativeAnchorTime;
				relativeAnchorTime = anchor;
				System.out.println(label);
				System.out.println("  --> Time from START: " + absoluTime);
				System.out.println("  --> Time from last operation: " + relativeTime);
				System.out.println();
			}
		}
	}
	
	public void printEnd() {
		System.out.println("  ->> END: " + new GregorianCalendar().getTime());
		System.out.println();
	}
	
}
