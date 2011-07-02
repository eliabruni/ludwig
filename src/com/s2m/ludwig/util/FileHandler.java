package com.s2m.ludwig.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;


public class FileHandler {

	/**
	 * 
	 * @param filePath
	 * @return
	 * @throws java.io.IOException
	 */
	public static String readFileAsString(String filePath) throws java.io.IOException{
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }
	
	/**
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static Set<String> readFileAsSet(String filePath) throws IOException{
		String text = readFileAsString(filePath);
		TreeSet<String> list = new TreeSet<String>();
		
		for(String token: text.split("\n")){
			list.add(token.trim());
		}
	
		return list;
	}
	
}
