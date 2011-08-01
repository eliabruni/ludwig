package com.s2m.ludwig.core.docs;

import java.io.IOException;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongSet;
import com.s2m.ludwig.persister.doc.DocDictionary;

public class Docs {

	/**
	 * The hbase-dictionary that retains String --> long mapping for each doc.
	 */
	private DocDictionary docDictionary;
	
	/**
	 * The set of documents consumed by each doc.
	 */
	private LongObjectOpenHashMap<LongSet> docDocuments;
	
	/**
	 * The bag of words associated with each doc.
	 */
	private LongObjectOpenHashMap<LongIntOpenHashMap> docBow;


	public Docs() throws IOException {
		// TODO: not so cool to throw an exception from the constructor!
		docDictionary = new DocDictionary();
		docDocuments = new LongObjectOpenHashMap<LongSet>();
		docBow = new LongObjectOpenHashMap<LongIntOpenHashMap>();
	}
	
	/**
	 * Return the long docId associated with the doc name (in String form).
	 * @param String doc
	 * @return long docId
	 */
	public long getDocId(String doc) {
		return docDictionary.convert(doc);
	}
	
	
	
	
	







}
