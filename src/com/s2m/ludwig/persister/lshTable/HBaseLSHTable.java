package com.s2m.ludwig.persister.lshTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseLSHTable {

	private static final long BUFFERSIZE = 65536;
	private static final byte[] SUMARRAYS = Bytes.toBytes("SumArrays");
	
	private HTable htable;

	public HBaseLSHTable() throws IOException {
		// TODO: pass to HTablePool
		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "SumArrays");
		htable.setWriteBufferSize(BUFFERSIZE);
	}

	public Put flushWord(long word, float[] sumArrays) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(sumArrays.length * 4);

		for (float f : sumArrays) {
			buffer.putFloat(f);
		}
		buffer.flip();
		byte[] rowkey = Bytes.toBytes(word);
		Put put = new Put(rowkey);

		put.add(SUMARRAYS, SUMARRAYS, buffer.array());
		
		return put;
	}
	
	
	public void flushWords(List<Put> puts) throws IOException {
		htable.put(puts);
	}
	


}
