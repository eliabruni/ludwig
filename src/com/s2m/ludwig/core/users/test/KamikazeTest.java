package com.s2m.ludwig.core.users.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.lucene.search.DocIdSetIterator;

import com.carrotsearch.hppc.IntArrayList;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.s2m.ludwig.core.cooccur.test.Timer;



// In a first test, the traditional approach seems much faster (1/3).

public class KamikazeTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		Timer timer = new Timer();
		timer.printTime("start");
		
		IntArrayList start_list = new IntArrayList();

		for (int i = 0; i < 10000000; i++) {
			start_list.add(i);
		}

		timer.printTime("list populated");

		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX KAMIKAZE APPROACH XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

		DocSet pForDeltaDocSet = DocSetFactory.getPForDeltaDocSetInstance();  

		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		// XXXX Iterating over LongArrayList XXXX
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
		// For the fastest iteration, you can access the lists' data buffer directly.
		final int [] buffer = start_list.buffer;

		// Make sure you take the list.size() and not the length of the data buffer.
		final int size = start_list.size();

		// Iterate of the the array as usual.
		for (int i = 0; i < size; i++) {
			pForDeltaDocSet.addDoc(buffer[i]);
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);


		oos.writeObject(pForDeltaDocSet);
		oos.flush();
		oos.close();

		byte[] pForDeltaDocSetToByteArray = bos.toByteArray();

		ByteArrayInputStream bis = new ByteArrayInputStream(pForDeltaDocSetToByteArray);
		ObjectInputStream ois = new ObjectInputStream(bis);

		DocSet newPForDeltaDocSet = (DocSet) ois.readObject();
		DocIdSetIterator iter = newPForDeltaDocSet.iterator();

		IntArrayList end_list1 = new IntArrayList();
		int docId = iter.nextDoc();

		while(docId !=DocIdSetIterator.NO_MORE_DOCS) 
		{      
			end_list1.add(docId);
			docId = iter.nextDoc();
		}

		timer.printTime("kamikaze terminated");
		

		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX TRADITIONAL APPROACH XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

		ByteBuffer buf = ByteBuffer.allocate(start_list.size() * 4);
		

		// Iterate of the the array as usual.
		for (int i = 0; i < size; i++) {
			buf.putInt(buffer[i]);
		}
		
		buf.flip();

		IntArrayList end_list2 = new IntArrayList();
		ByteBuffer buffer2 = ByteBuffer.wrap(buf.array());
		int pointer = 0;
		while (buffer2.hasRemaining()) {
			end_list2.add(buffer2.getInt());
			pointer++;
		}

		timer.printTime("traditional terminated");

	}

}
