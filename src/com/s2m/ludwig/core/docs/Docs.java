package com.s2m.ludwig.core.docs;

import java.io.IOException;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.s2m.ludwig.persister.doc.DocDictionary;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
public class Docs {

	/**
	 * The hbase-dictionary that retains String --> long mapping for each doc.
	 */
	private DocDictionary docDictionary;

	/**
	 * The list of users that consumed a certain doc.
	 */
	private LongObjectOpenHashMap<LongOpenHashSet> docsUsers;
	
	/**
	 * The bag of words associated with each doc.
	 */
	private LongObjectOpenHashMap<LongIntOpenHashMap> docsBOW;

	public Docs() throws IOException {
		// TODO: not so cool to throw an exception from the constructor!
		docDictionary = new DocDictionary();
		docsUsers = new LongObjectOpenHashMap<LongOpenHashSet>();
		docsBOW = new LongObjectOpenHashMap<LongIntOpenHashMap>();
	}

	/**
	 * Return the long docId associated with the doc name (in String form).
	 * @param String doc
	 * @return long docId
	 */
	public long getDocId(String doc) {
		return docDictionary.convert(doc);
	}
	
	/**
	 * 
	 * Add a docId to the set of its own consumed docs.
	 * 
	 * @param long docId
	 * @param long userId
	 */
	public void addUserToDoc(long docId, long userId) {
		LongOpenHashSet docUsers;
		if (docsUsers.containsKey(docId)) {
			if ((docUsers = docsUsers.get(docId)) != null) {
				docUsers.add(userId);
				docsUsers.put(docId, docUsers);
			}
		}
		else {
			docUsers = new LongOpenHashSet();
			docUsers.add(userId);
			docsUsers.put(docId, docUsers);
			
		}
	}
	
	/**
	 * 
	 * If there is no BOW associated with the current doc, a new BOW is created. 
	 * Then the count for the current word is updated.
	 * 
	 * @param long docId
	 * @param long term
	 */
	public void addTermToDocBOW(long docId, long term) {
		// TODO: Should be here the bow associated with a doc maintained once for all? 
		if (docsBOW.containsKey(docId)) {
			LongIntOpenHashMap docBOW = docsBOW.get(docId);
			if(docBOW.containsKey(term)) {
				docBOW.put(term, docBOW.get(term) + 1);
			}
			else {
				docBOW.put(term, 1);
			}
		}
		else {
			LongIntOpenHashMap docBOW = new LongIntOpenHashMap();
			docBOW.put(term, 1);
			docsBOW.put(docId, docBOW);
		}
	}

	
}
