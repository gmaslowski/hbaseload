// CsvInputStream.java - read data from a CSV formatted file (ref http://tools.ietf.org/html/rfc4180)
// Copyright (c) 2011 Niall McCarroll  
// Distributed under the MIT/X11 License (http://www.mccarroll.net/snippets/license.txt)

package net.mccarroll.hbaseload;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class CsvInputStream extends CsvStream implements Closeable {
	
	InputStream in = null;
	BufferedReader bufferedReader = null;
	
	public CsvInputStream(InputStream in) {
		this.in = in;
	}
	
	private void init() throws IOException {
		InputStreamReader streamReader = new InputStreamReader(in, encoding);
		bufferedReader = new BufferedReader(streamReader);
	}
	
	public static String[] parseCsvLine(char delimiter, BufferedReader reader) throws IOException {
		String line = getLine(reader);
		if (line == null) {
			return null;
		}
		
		List<String> vals = new ArrayList<String>();
		boolean inquotes = false;
		StringBuffer val = new StringBuffer();
		
		do {
			int i = 0;
			while(i<line.length()) {
				char c = line.charAt(i);
				char nc = (i<line.length()-1) ? line.charAt(i+1) : '\0';
				switch(c) {
					case '"':
						if (!inquotes) {
							val.setLength(0);
							inquotes = true;
						} else if (nc == '"') {
							val.append('"');
							i += 1;
						} else {
							inquotes = false;
							while(i < line.length()) {
								if (line.charAt(i) == delimiter) {
									vals.add(val.toString());
									val.setLength(0);
									break;
								}
								i++;
							}
							
						}
						break;
					
					default:
						if (c == delimiter) {
							if (!inquotes) {
								vals.add(val.toString());
								val.setLength(0);
							} else {
								val.append(c);
							}
						} else {
							if ((c != '\n' && c != '\r') || inquotes) {
								val.append(c);
							}
						}
						break;
				}
				i+=1;
			}	
			
			if (inquotes) {
				line = getLine(reader);
				if (line == null) {
					return null;
				}
			} 
		} while(inquotes);
		
		vals.add(val.toString());
		return vals.toArray(new String[0]);
	}
	
	public String[] readLine() throws IOException {
		if (bufferedReader == null) {
			init();
		}
		return parseCsvLine(delimiter,bufferedReader);
	}
	
	private static String getLine(BufferedReader reader) throws IOException {
		StringBuffer sb = new StringBuffer();
		int i;
		while((i=reader.read())>=0) {
			char c = (char)i;
			sb.append(c);
			if (c == '\n') {
				return sb.toString();
			}
		}
		if (sb.length()>0) {
			return sb.toString();
		} else {
			return null; // EOS
		}
	}
	
	public void close() throws IOException {
		if (bufferedReader != null) {
			bufferedReader.close();
		}
	}

}
