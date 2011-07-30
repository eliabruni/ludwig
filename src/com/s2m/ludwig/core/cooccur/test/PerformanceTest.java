package com.s2m.ludwig.core.cooccur.test;


import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;

public class PerformanceTest {

	
	public static void main(String[] args) {
		
		LongArrayList list = new LongArrayList();
		LongOpenHashSet set = new LongOpenHashSet();
		
		Timer timer = new Timer();
		timer.printTime("start");
		
		for (long i = 0; i < 10000000; i++) {
			list.add(i);
		}
		
		timer.printTime("list populated");
		
		
		for (long i = 0; i < 10000000; i++) {
			set.add(i);
		}
		
		timer.printTime("set populated");
		
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		// XXXX Iterating over LongArrayList XXXX
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		// For the fastest iteration, you can access the lists' data buffer directly.
		final long [] buffer = list.buffer;
		
		// Make sure you take the list.size() and not the length of the data buffer.
		final int size = list.size();
		
		// Iterate of the the array as usual.
		for (int i = 0; i < size; i++) {
		    long a = buffer[i];
		}
		
		timer.printTime("list iterated");
		
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		// XXXX Iterating over LongOpenHashSet XXXX
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

		// For the fastest iteration, you can access the sets's data buffers directly.
		final long [] keys = set.keys;
		final boolean [] allocated = set.allocated;

		// Note that the loop is bounded by states.length, not keys.length. This
		// can make the code faster due to range check elimination
		// (http://wikis.sun.com/display/HotSpotInternals/RangeCheckElimination).
		for (int i = 0; i < allocated.length; i++) {
		    if (allocated[i]) {
		        long a = keys[i];
		    }
		}
		
		timer.printTime("set iterated");

		
	}
	
}
