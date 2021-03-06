package com.s2m.ludwig.persister.users;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;
import com.s2m.ludwig.persister.cooccur.hdictionary.Dictionary;
import com.s2m.ludwig.persister.cooccur.hdictionary.NotPossibleException;


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
public class UserDictionary implements Dictionary {
	private static final Logger LOG = Logger.getLogger(UserDictionary.class);
	final private byte[] DICTIONARY_COLUMN_FAMILY = Bytes.toBytes("Dictionary");
	final private byte[] DICTIONARY_COLUMN = Bytes.toBytes("v");
	final private byte[] COUNTER_KEY = Bytes.toBytes("_counter");
	private HTable htable;
	private MemCachedClient mcc;
	
	public UserDictionary() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "UserDictionary");
		startMemCache();
	}
	

	// Set up connection pool once at class load
	private void startMemCache() {

		mcc = new MemCachedClient();
		
		// Server list and weights
		String[] servers = { "localhost:11211" };

		// The cache capacity 
		Integer[] weights = { 1 };

		// Grab an instance of our connection pool
		SockIOPool pool = SockIOPool.getInstance();

		// Set the servers and the weights
		pool.setServers( servers );
		pool.setWeights( weights );

		// Set some basic pool settings
		// 5 initial, 5 min, and 250 max conns
		// and set the max idle time for a conn
		// to 6 hours
		pool.setInitConn( 5 );
		pool.setMinConn( 5 );
		pool.setMaxConn( 250 );
		pool.setMaxIdle( 1000 * 60 * 60 * 6 );

		// set the sleep for the maint thread
		// it will wake up every x seconds and
		// maintain the pool size
		pool.setMaintSleep( 30 );

		// Set some TCP settings
		// disable nagle
		// set the read timeout to 3 secs
		// and don't set a connect timeout
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.setSocketConnectTO( 0 );

		// Initialize the connection pool
		pool.initialize();
		
		// Lets set some compression on for the client
		// compress anything larger than 64k
		//mcc.setCompressEnable( true );
		//mcc.setCompressThreshold( 64 * 1024 );
	}
	
	
	public long convert(String user) {
		Long value = (Long) mcc.get(user);
		//long value = mcc.getCounter(user);
		
		if (value == null) {
			value = getLong(user);
			
			if(value == 0){
				value = insert(user);
				
				// TODO: expiration time?
				mcc.set(user, value);
				//mcc.addOrIncr(user, value);
			}
		}
		
		return value;
	}

	private long insert(String user) {
		long value;
		try {
			value = htable.incrementColumnValue(COUNTER_KEY, DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, 1, true);

			byte[] rowkey = Bytes.toBytes(user);
			Put p = new Put(rowkey);
			p.add(DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, Bytes.toBytes(value));
			// somebody put the new key before us, let's get their value.
			if(!htable.checkAndPut(rowkey, DICTIONARY_COLUMN_FAMILY, DICTIONARY_COLUMN, null, p)) {
				LOG.warn("Concurrent insert, we lost a counter with user "+ user);
				value = getLong(user);
			}
		} catch (IOException e) {
			throw new NotPossibleException("Hbase's IOException", e);
		}
		
		return value;
	}

	private long getLong(String user) {
		long value = 0;
		Result res;
		Get g = new Get(Bytes.toBytes(user));
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