// HBaseImporter.java - import data to an HBase table from a CSV formatted file 
// Copyright (c) 2011 Niall McCarroll  
// Distributed under the MIT/X11 License (http://www.mccarroll.net/snippets/license.txt)

package net.mccarroll.hbaseload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

public class HBaseImporter extends HBaseImportExport {
	
	HBaseAdmin admin;
	Configuration config;
	Set<String> families = new HashSet<String>();
	List<HBaseCol> columns = new ArrayList<HBaseCol>();
	String tableName;
	int keyPosition = -1;
	
	public HBaseImporter(String tableName) {
		this.tableName = tableName;
	}
	
	public void init() throws IOException {
		config = HBaseConfiguration.create();  
		admin = new HBaseAdmin(config);
	}
	
	private void deleteTable() {
		try {
			admin.disableTable(tableName);    
			admin.deleteTable(tableName);
		} catch(Exception e) {
		}
	}
	
	private void createTable() throws IOException {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		admin.createTable(desc);
		admin.disableTable(tableName);
		for(String family: families) {
			HColumnDescriptor cf1 = new HColumnDescriptor(family);
			admin.addColumn(tableName, cf1);      
		}
		admin.enableTable(tableName); 
	}
	
	private void analyzeHeaders(String []headers, String keyColumn) {
		columns.clear();
		families.clear();
		int col = 0;
		for(String header: headers) {
			String family = DEFAULT_COLUMN_FAMILY;
			String qualifier = header;
			int pos;
			if ((pos = header.indexOf(":")) > 0) {
				family = header.substring(0,pos);
				qualifier = header.substring(pos+1);
			}
			columns.add(new HBaseCol(family,qualifier));
			families.add(family);
			if (header.equals(keyColumn)) {
				keyPosition = col;
			}
			col++;
		}
	}
	
	private void loadData(CsvInputStream cis) throws IOException {
		
		HTable table = new HTable(config,tableName); 
		
		String vals[] = cis.readLine();
		
		Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());
		int counter = 0;
		String rowId = "";
		while(vals != null) {
			if (keyPosition >= 0 && keyPosition < vals.length) {
				rowId = vals[keyPosition];
			} else {
				rowId = "r"+counter;
			}
			Put put = new Put(rowId.getBytes("UTF-8"));
			
			int col = 0;
			for(HBaseCol column: columns) {
				if (col >= vals.length) {
					break;
				}
				put.add(column.family.getBytes("UTF-8"), column.qualifier.getBytes(),vals[col].getBytes()); 
				col += 1;
			}
			table.put(put);
			vals = cis.readLine();
			counter += 1;
			if (counter % 10000 == 0) {
				logger.info("Imported "+counter+" records");
			}
		}
		cis.close();
	}

	/**
	 * import CSV to an HBase table
	 * 
	 * @param tableName name of the table in HBase
	 * @param csvFile a file
	 * 
	 * @throws IOException
	 */
	public void importCSV(File csvFile, String keyColumn) throws IOException {
		init();
		
		FileInputStream fis = new FileInputStream(csvFile);
		CsvInputStream cis = new CsvInputStream(fis);
		
		// read field names from the first line of the csv file
		analyzeHeaders(cis.readLine(),keyColumn);
		
		deleteTable();
		createTable();
		loadData(cis);
		cis.close();
	}
		
	public static void main(String[] args) throws IOException {
		if (args.length < 2 || args.length > 3) {
			System.out.println("Usage: HBaseImporter <tablename> <csv file path> [<key field name>]");
		}
		
		String tableName = args[0];
		File f = new File(args[1]);
		String keyColumn = null;
		if (args.length > 2) {
			keyColumn = args[2];
		}
		HBaseImporter importer = new HBaseImporter(tableName);
		importer.importCSV(f,keyColumn);
	}

}
