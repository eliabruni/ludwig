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
	
}
