package com.s2m.ludwig.persister.cooccur.sink;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;


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
public class CooccursSink {
	private static final long BUFFERSIZE = 65536;
	private static final byte[] COOCCUR = Bytes.toBytes("Cooccur");
	private HTable htable;
	
	public CooccursSink() throws IOException {
		// TODO: pass to HTablePool
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "Cooccur");
		htable.setWriteBufferSize(BUFFERSIZE);
	}
	
	public void flushWord(long term, LongIntOpenHashMap cooccur) throws IOException {
		byte[] termToBytes = Bytes.toBytes(term);
		Increment increment = new Increment(termToBytes);

		long[] keys    = cooccur.keys;
		int[] values   = cooccur.values;
		boolean[] used = cooccur.allocated;
		
		for (int i = 0; i < used.length; i++)
		    if (used[i]) 
		    	increment.addColumn(COOCCUR, Bytes.toBytes(keys[i]), values[i]);
		
		htable.increment(increment);
	}
	
}
