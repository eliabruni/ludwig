package com.s2m.ludwig.core.cooccur;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.s2m.ludwig.core.docs.Docs;
import com.s2m.ludwig.core.users.Users;
import com.s2m.ludwig.persister.cooccur.hdictionary.TermDictionary;
import com.s2m.ludwig.util.FileHandler;

public class Cooccur {
	
	/**
	 * The size of cooccur window. 
	 */
	private int WINDOW_SIZE;

	/**
	 * The path were to find the stopword list. 
	 */
	private String STOPWORD_PATH;
	
	private Cooccurs cooccurs;
	
	private int numberOfTP;
	
	private Users users;
	
	private Docs docs;
	
	public Cooccur(Cooccurs cooccurs, int numberOfTP, Users users, Docs docs) {
		this.cooccurs = cooccurs;
		this.numberOfTP = numberOfTP;
		this.users = users;
		this.docs = docs;
	}
	
	/**
	 *  Preprocess, convert terms to longs and populate termsCooccurs with 
	 *  the cooccur counts of the given document.
	 */
	public int[] insertCooccur(String text, long docId, long userId, int[] TPCounters) {
		Set<String> stopWords = null; 

		try {
			// TODO:
			// Put in a config file and use preconditions
			stopWords = FileHandler.readFileAsSet(STOPWORD_PATH);
		} 

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Reader reader = new StringReader(text);
		TokenStream stream = new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_30, 
				stopWords).tokenStream("content", reader);


		// TODO: manage the case of an empty document
		long[] terms = null;
		try {
			terms = convert(stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < terms.length; i++) {
			long term = terms[i];

			// We split the last term % NUMBER_OF_TP digits of term, 
			// to determine the actual TP.
			int TP = (int) (term % numberOfTP);

			// Get the TPCooccur for this TP
			LongObjectOpenHashMap<LongIntOpenHashMap> cooccur = cooccurs.getCooccur(TP);

			// Get the counter for this TP
			int TPCounter = TPCounters[TP];

			if (cooccur.containsKey(term)) {
				LongIntOpenHashMap map = cooccur.get(term);
				// Return the counter updated from the next sift window
				TPCounters[TP] = siftWindow(terms, cooccur, TPCounter, i, term, userId, docId);
				// TODO: It's a bad way, find a more elegant
			} 

			else {
				LongIntOpenHashMap map = new LongIntOpenHashMap();
				// Return the counter updated from the next sift window
				TPCounters[TP] = siftWindow(terms, cooccur, TPCounter, i, term, userId, docId);
			}

		}
		
		return TPCounters;
	}
	
	


	/**********************************************************************************
	 * Cooccur helper functions
	 **********************************************************************************/

	/**
	 * 
	 * Count the coooccur for the given window
	 * 
	 * @param terms
	 * @param window
	 * @param i
	 * @param term
	 * @param map
	 */
	private int siftWindow(long[] terms, LongObjectOpenHashMap<LongIntOpenHashMap> cooccur, int TPCounter, 
			int i, long term, long userId, long docId) {
		
		// TODO: introduce check if cooccur contains term
		// In general maybe we got to rethink the way we store cooccur. Just one map for all.
		LongIntOpenHashMap map = cooccur.get(term);
		
		for (int j = i - WINDOW_SIZE; j < i + WINDOW_SIZE + 1; j++) {
			if (j == i || j < 0)
				continue;

			if (j >= terms.length)
				break;

			if (map.containsKey(terms[j])) {
				map.put(terms[j], map.get(terms[j]) + 1);
				cooccur.put(term, map);
				// Here we use term to update the user BOW, because term is the pivot of the window.
				users.addTermToUserBOW(userId, term);
				// Here we use term to update the doc's BOW, because term is the pivot of the window.
				docs.addTermToDocBOW(docId, term);
			} 

			else {
				map.put(terms[j], 1);
				cooccur.put(term, map);
				// Here we use term to update the user's BOW, because term is the pivot of the window.
				users.addTermToUserBOW(userId, term);
				// Here we use term to update the doc's BOW, because term is the pivot of the window.
				docs.addTermToDocBOW(docId, term);
				// We increment the counter only here, because it's here we're occupying new space.
				// TODO: Calculate the exact amount of space we're increasing the map here, in order to 
				//       decide when to flush on space-based.
				TPCounter++;
			}
		}

		return TPCounter;
	}
	
	/**
	 * 
	 * Map String --> long, through a HBasedictionary and a cache.
	 * @throws IOException 
	 * 
	 */
	private long[] convert(TokenStream stream) throws IOException {

		TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
		LongArrayList terms = new LongArrayList();
		TermDictionary dic = new TermDictionary();

		try {
			while (stream.incrementToken()) {
				String stringTerm = termAtt.term();
				terms.add(dic.convert(stringTerm));
			}
		} 

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return terms.toArray();
	}

}
