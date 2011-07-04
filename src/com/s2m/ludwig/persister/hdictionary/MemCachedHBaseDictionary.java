package com.s2m.ludwig.persister.hdictionary;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;



public class MemCachedHBaseDictionary implements Dictionary {
	private static final Logger LOG = Logger.getLogger(MemCachedHBaseDictionary.class);
	final private byte[] DICTIONARY_COLUMN_FAMILY = Bytes.toBytes("Dictionary");
	final private byte[] DICTIONARY_COLUMN = Bytes.toBytes("v");
	final private byte[] COUNTER_KEY = Bytes.toBytes("_counter");
	private HTable htable;
	private MemcachedClient cache;
	// TODO: to put in configuration
	private int EXP_TIME = 3*3600;
	
	public MemCachedHBaseDictionary() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "SemGraphDictionary");
		cache  = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses("localhost:11211"));
	}
	
	public long convert(String word) {
		Long value = null;
		Future<Object> f = cache.asyncGet(word);
		
		try {
		    value = (Long) f.get(5, TimeUnit.SECONDS);
		} catch(TimeoutException e) {
		    f.cancel(false);
		} catch(InterruptedException e) {
			throw new NotPossibleException("Interrupted get", e);
		} catch(ExecutionException e) {
			throw new NotPossibleException("Execution exception in get", e);
		}

		if(value == null) {
			value = getLong(word);
			
			if(value == 0){
				value = insert(word);
				cache.add(word, EXP_TIME, value);
			}
		}
		
		return value;
	}

	private long insert(String word) {
		long value;
		try {
			value = htable.incrementColumnValue(COUNTER_KEY, DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, 1, true);

			byte[] rowkey = Bytes.toBytes(word);
			Put p = new Put(rowkey);
			p.add(DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, Bytes.toBytes(value));
			// somebody put the new key before us, let's get their value.
			if(!htable.checkAndPut(rowkey, DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, null, p)){
				LOG.warn("Concurrent insert, we lost a counter with word "+ word);
				value = getLong(word);
			}
		} catch (IOException e) {
			throw new NotPossibleException("Hbase's IOException", e);
		}
		
		return value;
	}

	private long getLong(String word) {
		long value = 0;
		Result res;
		Get g = new Get(Bytes.toBytes(word));
		g.addColumn(DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN);
		
		try {
			res = htable.get(g);
		} catch (IOException e) {
			throw new NotPossibleException("HBase's IOException", e);
		}
		
		if(!res.isEmpty()){
			value = Bytes.toLong(res.getValue(DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN));
		}
		
		return value;
	}	
}