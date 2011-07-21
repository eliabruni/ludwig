package com.s2m.ludwig.core.test;

import java.nio.ByteBuffer;

import kafka.producer.Producer;

public class GeneralTest {

	public static void main(String[] args) {
		
		float[] floats = {1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f};
		ByteBuffer buffer = ByteBuffer.allocate(floats.length * 4);
		
		for (float f : floats) {
			buffer.putFloat(f);
		}
		buffer.flip();
		
		ByteBuffer buffer2 = ByteBuffer.wrap(buffer.array());
		float[] sumArray = new float[buffer.capacity()/4];
		int pointer = 0;
		while (buffer2.hasRemaining()) {
			sumArray[pointer] = buffer2.getFloat();
			pointer++;
		}
		
		for (float f: sumArray) {
			System.out.println(f);
		}
	}
	
	
}
