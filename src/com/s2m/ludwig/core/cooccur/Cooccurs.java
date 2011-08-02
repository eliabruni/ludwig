package com.s2m.ludwig.core.cooccur;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;


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
public class Cooccurs {
	
	/**
	 * Retain all the cooccur counts.
	 */
	private ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>> cooccurs;
	
	
	/**********************************************************************************
	 * Constructors
	 **********************************************************************************/

	public Cooccurs() {
		cooccurs = new ObjectArrayList<LongObjectOpenHashMap<LongIntOpenHashMap>>();
	}
	

	/**********************************************************************************
	 * Main methods
	 **********************************************************************************/
	/**
	 * 
	 */
	public void addCooccurMap(LongObjectOpenHashMap<LongIntOpenHashMap> cooccur) {
		cooccurs.add(cooccur);
	}
	
	/**
	 * 
	 * @param TP
	 * @return
	 */
	public LongObjectOpenHashMap<LongIntOpenHashMap> getCooccur(int TP) {
		return cooccurs.get(TP);
	}
	
	/**
	 * 
	 * @param TP
	 */
	public void removeTP(int TP) {
		// TODO: put a check here, to be sure there is the TP inside cooccurs, before removing.
		cooccurs.remove(TP);
	}
	
	/**
	 * 
	 * @param TP
	 */
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
	
}
