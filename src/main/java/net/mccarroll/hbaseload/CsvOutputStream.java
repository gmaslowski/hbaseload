// CsvOutputStream.java - write data to a CSV formatted file (ref http://tools.ietf.org/html/rfc4180)
// Copyright (c) 2011 Niall McCarroll  
// Distributed under the MIT/X11 License (http://www.mccarroll.net/snippets/license.txt)

package net.mccarroll.hbaseload;

import java.io.*;

public class CsvOutputStream extends net.mccarroll.hbaseload.CsvStream implements Closeable {
	
	OutputStream out;
	BufferedWriter bufferedWriter;

	public CsvOutputStream(OutputStream out) {
		this.out = out;
	}
	
	public void init() throws UnsupportedEncodingException {
		OutputStreamWriter streamWriter = new OutputStreamWriter(out, encoding);
		bufferedWriter = new BufferedWriter(streamWriter);
	}
	
	public static String dumpCsvLine(String []vals, char delimiter) {
		StringBuffer s = new StringBuffer();
		for(int i=0; i<vals.length; i++) {
			if (i>0) {
				s.append(delimiter);
			}
			s.append(dumpCsvValue(vals[i],delimiter));
		}
		return s.toString();
	}
	
	public static String dumpCsvValue(String value, char delimiter) {
		if (value.indexOf(delimiter)>=0 || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
			return "\""+value.replace("\"", "\"\"")+"\"";
		} else {
			return value;
		}
	}
	
	public void writeLine(String[]vals) throws IOException {
		if (bufferedWriter == null) {
			init();
		}
		bufferedWriter.write(dumpCsvLine(vals,delimiter)+"\n");
	}
	
	public void close() throws IOException {
		bufferedWriter.close();
	}
}
