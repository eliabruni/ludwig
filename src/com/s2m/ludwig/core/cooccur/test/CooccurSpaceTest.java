package com.s2m.ludwig.core.cooccur.test;

import com.carrotsearch.hppc.ObjectArrayList;

public class CooccurSpaceTest {

	public static void main(String[] args) {

		System.out.println("begin");
		int[] test = new int[10];
		for (int i : test) {
			System.out.println(i);
		}
		
		System.out.println("increment without assignment");
		increment(test);
		
		for (int i : test) {
			System.out.println(i);
		}
		
		System.out.println("increment with assignment");
		
		test = increment(test);
		
		for (int i : test) {
			System.out.println(i);
		}
		
		Integer i = 1;
		
		System.out.println("i before: " + i);
		
		i = increment(i);
		
		System.out.println("i after: " + i);
		
		
	}
	
	private static int[] increment(int[] test) {
		for (int i = 0; i < test.length; i++) {
			test[i] = test[i] + 10;
		}
		return test;
	}
	
	private static Integer increment(Integer i) {
		i += 1;
		return i;
	}


}
