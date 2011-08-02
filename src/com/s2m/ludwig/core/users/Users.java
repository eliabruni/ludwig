package com.s2m.ludwig.core.users;

import java.io.IOException;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.s2m.ludwig.persister.users.UserDictionary;


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
public class Users {

	/**
	 * The hbase-dictionary that retains String --> long mapping for each user.
	 */
	private UserDictionary userDictionary;
	
	/**
	 * The set of documents consumed by each user.
	 */
	private LongObjectOpenHashMap<LongOpenHashSet> usersDocs;
	
	/**
	 * The bag of words associated with each user.
	 */
	private LongObjectOpenHashMap<LongIntOpenHashMap> usersBOW;


	public Users() throws IOException {
		// TODO: not so cool to throw an exception from the constructor!
		userDictionary = new UserDictionary();
		usersDocs = new LongObjectOpenHashMap<LongOpenHashSet>();
		usersBOW = new LongObjectOpenHashMap<LongIntOpenHashMap>();
	}
	
	/**
	 * 
	 * Return the long userId associated with the user name (in String form).
	 * 
	 * @param String user
	 * @return long userId
	 */
	public long getUserId(String user) {
		return userDictionary.convert(user);
	}
	
	/**
	 * 
	 * Add a docId to the set of its own consumed docs.
	 * 
	 * @param long userId
	 * @param long docId
	 */
	public void addDocToUser(long userId, long docId) {
		LongOpenHashSet userDocs;
		if (usersDocs.containsKey(userId)) {
			if ((userDocs = usersDocs.get(userId)) != null) {
				userDocs.add(docId);
				usersDocs.put(userId, userDocs);
			}
		}
		else {
			userDocs = new LongOpenHashSet();
			userDocs.add(docId);
			usersDocs.put(userId, userDocs);
			
		}
	}
	
	/**
	 * 
	 * If there is no BOW associated with the current user, a new BOW is created. 
	 * Then the count for the current word is updated.
	 * 
	 * @param long userId
	 * @param long term
	 */
	public void addTermToUserBOW(long userId, long term) {
		if (usersBOW.containsKey(userId)) {
			LongIntOpenHashMap userBOW = usersBOW.get(userId);
			if(userBOW.containsKey(term)) {
				userBOW.put(term, userBOW.get(term) + 1);
			}
			else {
				userBOW.put(term, 1);
			}
		}
		else {
			LongIntOpenHashMap userBOW = new LongIntOpenHashMap();
			userBOW.put(term, 1);
			usersBOW.put(userId, userBOW);
		}
	}

}
