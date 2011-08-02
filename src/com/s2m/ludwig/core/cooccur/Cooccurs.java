package com.s2m.ludwig.core.cooccur;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.s2m.ludwig.conf.LudwigConfiguration;
import com.s2m.ludwig.core.docs.Docs;
import com.s2m.ludwig.core.users.Users;
import com.s2m.ludwig.persister.cooccur.hdictionary.TermDictionary;
import com.s2m.ludwig.util.FileHandler;

public class Cooccurs {
	
	/**
	 * Retain all the cooccur counts. X
	 */
	private ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>> cooccurs;

	/**
	 * The size of cooccur window. X
	 */
	private int WINDOW_SIZE;

	/**
	 * The path were to find the stopword list. X
	 */
	private String STOPWORD_PATH;
	
	static LudwigConfiguration conf = LudwigConfiguration.get();
	
	public Cooccurs() {
		this(conf.getWindowSize(), conf.getStopwordsPath());
	}
	
	public Cooccurs(int WINDOW_SIZE, String STOPWORD_PATH) {
		this.WINDOW_SIZE = WINDOW_SIZE;
		this.STOPWORD_PATH = STOPWORD_PATH;
		cooccurs = new ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>>();
	}
	
	public void addCooccurMap(LongObjectOpenHashMap<LongIntOpenHashMap> cooccur) {
		cooccurs.add(cooccur);
	}
	
	public LongObjectOpenHashMap<LongIntOpenHashMap> getCooccur(int TP) {
		return cooccurs.get(TP);
	}
	
	public void removeTP(int TP) {
		// TODO: put a check here, to be sure there is the TP inside cooccurs, before removing.
		cooccurs.remove(TP);
	}
	
	public void insertTP(int TP) {
		cooccurs.insert(TP, new LongObjectOpenHashMap<LongIntOpenHashMap>());
	}
	
	/**
	 * 
	 * A wrapper around removeTP(int TP) and insertTP(int TP).
	 * 
	 * @param int TP
	 */
	public void resetTP(int TP) {
		removeTP(TP);
		insertTP(TP);
	}
	
	public int getWindowSize() {
		return WINDOW_SIZE;
	}
	
	public String getStopwordPath() {
		return STOPWORD_PATH;
	}
	
	/**
	 *  Preprocess, convert terms to longs and populate termsCooccurs with 
	 *  the cooccur counts of the given document.
	 */
	protected void insertCooccur(String text, Users users, Docs docs, long docId, long userId, int numberOfTP, long cooccursCounter) {
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
			LongObjectOpenHashMap<LongIntOpenHashMap> TPCoccour = getCooccur(TP);



			if (TPCoccour.containsKey(term)) {
				LongIntOpenHashMap map = TPCoccour.get(term);
				// Return the counter updated from the next sift window
				cooccursCounter += siftWindow(terms, users, docs, TPCoccour, i, term, map, cooccursCounter, userId, docId);
				// TODO: It's a bad way, find a more elegant
			} 

			else {
				LongIntOpenHashMap map = new LongIntOpenHashMap();
				// Return the counter updated from the next sift window
				cooccursCounter += siftWindow(terms, users, docs, TPCoccour, i, term, map, cooccursCounter, userId, docId);
			}

			/*// If the counter of this TP reaches the threshold,
			// the cooccur for this TP are serialized and sent to
			// the broker. All is reinitialized. 
			// TODO: is right to put a check here, or better a independent thread that
			//       every some time checks . Even better, we should put here two different 
			//       thresholds: time and count.  
			if (TPCounter >= TP_THRESHOLD) {
				// TODO: We have to send a list of TPs' cooccur, whose size will be decided
				//       by a certain threshold. Once reached we send the list of messages to the broker
				byte[] body = serialize(TP, TPCounter, cooccurs.getCooccur(TP)); 
				ArrayList<byte[]> message = new ArrayList<byte[]>();
				message.add(body);

				sendToBroker(TP, message);
				partitionCounter[TP] = 0;
			}*/
		}
	}

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
	private long siftWindow(long[] terms, Users users, Docs docs, LongObjectOpenHashMap<LongIntOpenHashMap> termsCooccur, 
			int i, long term, LongIntOpenHashMap map, long cooccursCounter, long userId, long docId) {
		
		for (int j = i - WINDOW_SIZE; j < i + WINDOW_SIZE + 1; j++) {
			if (j == i || j < 0)
				continue;

			if (j >= terms.length)
				break;

			if (map.containsKey(terms[j])) {
				map.put(terms[j], map.get(terms[j]) + 1);
				termsCooccur.put(term, map);
				// Here we use term to update the user BOW, because term is the pivot of the window.
				users.addTermToUserBOW(userId, term);
				// Here we use term to update the doc's BOW, because term is the pivot of the window.
				docs.addTermToDocBOW(docId, term);
				cooccursCounter++;
			} 

			else {
				map.put(terms[j], 1);
				termsCooccur.put(term, map);
				// Here we use term to update the user's BOW, because term is the pivot of the window.
				users.addTermToUserBOW(userId, term);
				// Here we use term to update the doc's BOW, because term is the pivot of the window.
				docs.addTermToDocBOW(docId, term);
				cooccursCounter++;
			}
		}

		return cooccursCounter;
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
