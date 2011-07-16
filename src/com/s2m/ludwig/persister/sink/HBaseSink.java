package com.s2m.ludwig.persister.sink;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.LongIntOpenHashMap;

public class HBaseSink {
	private static final long BUFFERSIZE = 65536;
	private static final byte[] COOCCURS = Bytes.toBytes("Cooccurs");
	private HTable htable;
	
	public HBaseSink() throws IOException {
		// TODO: pass to HTablePool
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "Cooccurs");
		htable.setWriteBufferSize(BUFFERSIZE);
	}
	
	public void flushWord(byte[] word, LongIntOpenHashMap cooccurs) throws IOException {
		Increment increment = new Increment(word);

		long[] keys    = cooccurs.keys;
		int[] values   = cooccurs.values;
		boolean[] used = cooccurs.allocated;
		
		for (int i = 0; i < used.length; i++)
		    if (used[i]) 
		    	increment.addColumn(COOCCURS, Bytes.toBytes(keys[i]), values[i]);
		
		htable.increment(increment);
	}
}
