package com.s2m.ludwig.persister.lshTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;


public class HBaseLSHTable {

	private static final long BUFFERSIZE = 65536;
	private static final byte[] SUMARRAYS = Bytes.toBytes("SumArrays");

	private HTable htable;

	private List<Put> puts;
	private List<Get> gets;

	public HBaseLSHTable() throws IOException {
		// TODO: pass to HTablePool
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "SumArrays");
		htable.setWriteBufferSize(BUFFERSIZE);
		puts = new ArrayList<Put>();
		gets = new ArrayList<Get>();
	}

	public void createSumArrayPut(long word, float[] sumArrays) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(sumArrays.length * 4);

		for (float f : sumArrays) {
			buffer.putFloat(f);
		}
		buffer.flip();
		byte[] rowkey = Bytes.toBytes(word);
		Put p = new Put(rowkey);

		p.add(SUMARRAYS, SUMARRAYS, buffer.array());
		puts.add(p);

	}


	public void flushSumArrays(List<Put> puts) throws IOException {
		htable.put(puts);
		puts.clear();
	}

	public void createSumArrayGet(long word) {
		Get g = new Get(Bytes.toBytes(word));
		g.addColumn(SUMARRAYS, SUMARRAYS);
		gets.add(g);
	}

	public LongObjectOpenHashMap<float[]> fetchSumArrays() throws IOException {
		LongObjectOpenHashMap<float[]> sumArrays = new LongObjectOpenHashMap<float[]>();
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
