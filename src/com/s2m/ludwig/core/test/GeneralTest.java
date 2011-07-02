package com.s2m.ludwig.core.test;

import kafka.producer.Producer;

public class GeneralTest {

	public static void main(String[] args) {
		
		Producer[] producers = new Producer[10];
		
		for (Producer producer : producers) {
			System.out.println(producer);
		}
		
	}
	
	
}
