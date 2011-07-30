package com.s2m.ludwig.persister.cooccur.lshTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.LongObjectOpenHashMap;

/**
 * 
 * This table is to store LSH d-dimensional vectors (sumArrays), aggregated at each collector's flush.
 * SumArrays are needed to compute terms' signatures for cosine similarity approximation.
 * 
 * @author eliabruni
 *
 */

public class LSHTable {
	private static HTablePool pool = new HTablePool();
	private static final long BUFFERSIZE = 65536;
	private static final byte[] SUMARRAYS = Bytes.toBytes("SumArrays");

	private HTable htable;
	private List<Put> puts;
	private List<Get> gets;

	public LSHTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "SumArrays");
		htable.setWriteBufferSize(BUFFERSIZE);
		puts = new ArrayList<Put>();
		gets = new ArrayList<Get>();
	}

	/**
	 * 
	 * Serialize a sumArray and create a Put to store it in HBase.
	 * 
	 * @param long word
	 * @param float[] sumArray
	 * @throws IOException
	 */
	public void createSumArrayPut(long word, float[] sumArray) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(sumArray.length * 4);

		for (float f : sumArray) {
			buffer.putFloat(f);
		}
		
		buffer.flip();
		byte[] rowkey = Bytes.toBytes(word);
		Put p = new Put(rowkey);
		p.add(SUMARRAYS, SUMARRAYS, buffer.array());
		puts.add(p);
	}

	/**
	 * 
	 * Flush all the sumArrays into HBase.
	 * 
	 * @param List<Put> puts
	 * @throws IOException
	 */
	public void flushSumArrays(List<Put> puts) throws IOException {
		HTableInterface htable = pool.getTable(SUMARRAYS);
		htable.put(puts);
		puts.clear();
	}

	/**
	 * 
	 * Create the Get to retrieve the sumArray associated to the given word.
	 * 
	 * @param long word
	 */
	public void createSumArrayGet(long word) {
		// TODO: Manage the case in which there is no sumArray for the given word
		Get g = new Get(Bytes.toBytes(word));
		g.addColumn(SUMARRAYS, SUMARRAYS);
		gets.add(g);
	}

	/**
	 * 
	 * Return all the sumArrays for which we asked for with createSumArrayGet.
	 * 
	 * @return LongObjectOpenHashMap<float[]> sumArrays
	 * @throws IOException
	 */
	public LongObjectOpenHashMap<float[]> fetchSumArrays() throws IOException {
		//TODO: >Claudio: no need to clear gets?
		LongObjectOpenHashMap<float[]> sumArrays = new LongObjectOpenHashMap<float[]>();
		HTableInterface htable = pool.getTable(SUMARRAYS);
		Result[] results = htable.get(gets);
		for (Result res : results) {
			byte[] byteTerm = res.getRow();
			long term = Bytes.toLong(byteTerm);
			byte[] byteSumArray = res.getValue(SUMARRAYS, SUMARRAYS);

			ByteBuffer buffer = ByteBuffer.wrap(byteSumArray);
			float[] sumArray = new float[buffer.capacity()/4];
			int pointer = 0;
			while (buffer.hasRemaining()) {
				sumArray[pointer] = buffer.getFloat();
				pointer++;
			}
			sumArrays.put(term, sumArray);
		}

		return sumArrays;
	}

}
