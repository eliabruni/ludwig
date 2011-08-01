package com.s2m.ludwig.core.users;

import java.io.IOException;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongSet;
import com.s2m.ludwig.persister.user.UserDictionary;

public class Users {

	/**
	 * The hbase-dictionary that retains String --> long mapping for each user.
	 */
	private UserDictionary userDictionary;
	
	/**
	 * The set of documents consumed by each user.
	 */
	private LongObjectOpenHashMap<LongSet> userDocuments;
	
	/**
	 * The bag of words associated with each user.
	 */
	private LongObjectOpenHashMap<LongIntOpenHashMap> userBOWs;


	public Users() throws IOException {
		// TODO: not so cool to throw an exception from the constructor!
		userDictionary = new UserDictionary();
		userDocuments = new LongObjectOpenHashMap<LongSet>();
		userBOWs = new LongObjectOpenHashMap<LongIntOpenHashMap>();
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
	 * If there is no BOW associated with the current user, a new BOW is created. 
	 * Then the count for the current word is updated.
	 * 
	 * @param long userId
	 * @param long term
	 */
	public void addTermToUserBOW(long userId, long term) {
		if (userBOWs.containsKey(userId)) {
			LongIntOpenHashMap BOW = userBOWs.get(userId);
			if(BOW.containsKey(term)) {
				BOW.put(term, BOW.get(term) + 1);
			}
			else {
				BOW.put(term, 1);
			}
		}
		else {
			LongIntOpenHashMap BOW = new LongIntOpenHashMap();
			BOW.put(term, 1);
			userBOWs.put(userId, BOW);
		}
	}
	
	
	
	
	







}
