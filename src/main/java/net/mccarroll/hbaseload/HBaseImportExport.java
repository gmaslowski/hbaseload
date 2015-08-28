// HBaseImportExport.java - common definitions for HBase/CSV import/export 
// Copyright (c) 2011 Niall McCarroll  
// Distributed under the MIT/X11 License (http://www.mccarroll.net/snippets/license.txt)

package net.mccarroll.hbaseload;

public class HBaseImportExport {

    public String DEFAULT_COLUMN_FAMILY = "c1";

    public class HBaseCol {
        String family;
        String qualifier;
        HBaseCol(String family,String qualifier) {
            this.family = family;
            this.qualifier = qualifier;
        }
    }

}