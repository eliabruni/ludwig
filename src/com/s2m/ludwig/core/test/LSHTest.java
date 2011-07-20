package com.s2m.ludwig.core.test;

import java.util.HashMap;

import com.s2m.ludwig.util.lsh.LSH;

public class LSHTest {
	
	public static void main(String[] args) {
		
		
		// Comparing two word cooccurrences
		
		String strTest = "test";
		String left = "left";
		String right = "right";
		String one = "one";
		String two = "two";
		
		HashMap<String, Double> features1 = new HashMap<String, Double>();
		features1.put(left, 1d);
		features1.put(right, 2d);
		features1.put(one, 1d);
		features1.put(two, 2d);
		
		LSH lsh1 = new LSH();
		
		lsh1.updateFeatures(features1);
		
		byte[] signature1 = lsh1.buildSignature();
		
		HashMap<String, Double> features2 = new HashMap<String, Double>();
		//features2.put("zorro", 1d);
		features2.put(left, 1d);
		features2.put(right, 2d);
		features2.put(one, 1d);
		features2.put(two, 2d);
		
		lsh1.resetSumArray();
		
		lsh1.updateFeatures(features2);
		
		byte[] signature2 = lsh1.buildSignature();
		
		System.out.println(lsh1.scoreSignatures(signature1, signature2));
		
		
		
		// Aggregating two d-dimensional vectors
		
		

		
		

	}

}
