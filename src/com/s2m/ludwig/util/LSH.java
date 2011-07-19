package com.s2m.ludwig.util;

import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

/**
 * Builds / compares signature vectors. 
 * Modified code of Benjamin Van Durme, vandurme@cs.jhu.edu
 */
public class LSH {

	private static final Log LOG = LogFactory.getLog(LSH.class);  
	/** Number of bits (b) */
	private static final int NUM_BITS = 256;//4096;//256;
	/**Size of the pool. */
	private static final int POOL_SIZE = 100000;//10000000;//100000;

	public LSH() {
		try {
			m_hashes = Hash.getRandomHashes(NUM_BITS);
			m_pool = new double[POOL_SIZE];

			Random random = new Random();

			for (int i = 0; i < m_pool.length; i++) {
				m_pool[i] = random.nextGaussian();
			}
		} catch (Exception e) {
			m_hashes = null;
			LOG.error("Failed to instantiate class", e);
		}
	}

	public byte[] buildSignature(HashMap<String, Double> features) {

		float[] sumArray = new float[NUM_BITS];
		byte[] sig = new byte[NUM_BITS/8];

		// Generate the counter array
		for (String feature : features.keySet()) {

			for (int i = 0; i < NUM_BITS; i++) {
				sumArray[i] += features.get(feature) * m_pool[Hash.hash(feature, m_hashes[i], m_pool.length)];
			}
		}

		// Build the signature
		int s,i,j;

		for (i = 0; i < NUM_BITS; i+=8) {
			s = 0;

			if (sumArray[i] > 0) {
				s = s | 1;
			}

			for (j = 1; j < 8; j++) {
				s = s << 1;
				if (sumArray[i+j] > 0) {
					s = s | 1;
				}
			}

			sig[i/8] = (byte)s;
		}

		return sig;
	}

	public void buildSignatures(LongObjectOpenHashMap<LongIntOpenHashMap> termsCooccurs) {

		float[] sumArray = new float[NUM_BITS];
		byte[] sig = new byte[NUM_BITS/8];


		final boolean outerStates[] = termsCooccurs.allocated;
		final long[] outerKeys = termsCooccurs.keys;

		for (int k = 0; k < outerStates.length; k++) {

			if(outerStates[k]) {
				long term = outerKeys[k];

				final boolean innerStates[] = termsCooccurs.get(outerKeys[k]).allocated;
				final long[] innerKeys = termsCooccurs.get(outerKeys[k]).keys;
				final int[] innerValues = termsCooccurs.get(outerKeys[k]).values;

				for (int j = 0; j < innerStates.length; j++) {
					if (innerStates[j]) {
						long otherTerm = innerKeys[j];
						int count = innerValues[j];

						for (int i = 0; i < NUM_BITS; i++) {
							// TODO:
							// Hash function for long with murmur hash has to be created
							// sumArray[i] += Math.log(Math.exp(sumArray[i]) + count) * m_pool[Hash.hash(feature, m_hashes[i], m_pool.length)];
							sumArray[i] += count * m_pool[Hash.hash(otherTerm, m_hashes[i], m_pool.length)];
						}

					}
				}

			}

			// Build the signature
			int s,i,j;

			for (i = 0; i < NUM_BITS; i+=8) {
				s = 0;

				if (sumArray[i] > 0) {
					s = s | 1;
				}

				for (j = 1; j < 8; j++) {
					s = s << 1;
					if (sumArray[i+j] > 0) {
						s = s | 1;
					}
				}

				sig[i/8] = (byte)s;
			}
			
			// TODO 
			// Here sig has to be concatenated with the old one in hbase (?)
			
		}

	}



	public static double scoreSignatures(byte[] sigX, byte[] sigY) {
		return LSHSignature.approximateCosine(sigX, sigY);
	}

	/** Pool of random numbers */
	private double[] m_pool;
	/** Hashes. */
	private int[] m_hashes;
}
