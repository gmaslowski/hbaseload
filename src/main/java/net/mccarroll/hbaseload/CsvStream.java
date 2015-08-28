// CsvStream.java - set configuration options for CSV format
// Copyright (c) 2011 Niall McCarroll  
// Distributed under the MIT/X11 License (http://www.mccarroll.net/snippets/license.txt)

package net.mccarroll.hbaseload;

public class CsvStream {
	char delimiter = ',';
	String encoding = "UTF-8";
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}
}
