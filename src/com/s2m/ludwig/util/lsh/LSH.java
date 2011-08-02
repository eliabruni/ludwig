package com.s2m.ludwig.util.lsh;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.s2m.ludwig.persister.cooccur.lsh.LSHSink;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  This class implements Online Generation of Locality Sensitive Hash Signature.
 *  See Van Durme and Lall, 2010 (http://www.cs.jhu.edu/~vandurme/papers/VanDurmeLallACL10.pdf).
 *  With NUM_BITS = 256 and POOL_SIZE = 10^5 we should obtain best cosine estimation,
 *  with mean absolute error ~ 0.055. Each sumArray occupies 8.192 bits (NUM_BITS*32), 
 *  by using float[] (See Fig. 3).
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
public class LSH {
	// TODO: Randomness between collectors must be tied based on a shared seed s.

	LSHSink lshSink;
	
	private static final Log LOG = LogFactory.getLog(LSH.class);  
	/** Number of bits (b)*/
	private static final int NUM_BITS = 256;
	/**Size of the pool*/
	private static final int POOL_SIZE = 100000;
	/** Pool of random numbers */
	private double[] m_pool;
	/** Hashes. */
	private int[] m_hashes;

	public LSH() throws IOException {
		try {
			m_hashes = LSHHash.getRandomHashes(NUM_BITS);
			m_pool = new double[POOL_SIZE];

			Random random = new Random();

			for (int i = 0; i < m_pool.length; i++) {
				m_pool[i] = random.nextGaussian();
			}
		} catch (Exception e) {
			m_hashes = null;
			LOG.error("Failed to instantiate class", e);
		}
		
		lshSink = new LSHSink();
	}

	public byte[] buildSignature(float[] sumArray) {
		byte[] sig = new byte[NUM_BITS/8];

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

	public void buildSumArray(long term, LongIntOpenHashMap cooccur) throws IOException {
		// TODO: need to put update/create-new sumArray with a check in LSHSink to see 
		//       if there's already a sumArray.   
		float[] sumArray = new float[NUM_BITS];

		long[] keys    = cooccur.keys;
		int[] values   = cooccur.values;
		boolean[] used = cooccur.allocated;

		for (int i = 0; i < used.length; i++) {
			if (used[i]) {
				long otherTerm = keys[i];
				int count = values[i];

				for (int j = 0; j < NUM_BITS; j++) {
					sumArray[j] += count * m_pool[LSHHash.Hash(otherTerm, m_hashes[j], m_pool.length)];
				}
			}

			lshSink.createSumArrayPut(term, sumArray);
		}
	}
	
	// TODO: find an other way instead of wrapping around LH=SHSink method
	public void flushSumrrays() throws IOException {
		lshSink.flushSumArrays();
	}

	public static double scoreSignatures(byte[] sigX, byte[] sigY) {
		return LSHSignature.approximateCosine(sigX, sigY);
	}

}
