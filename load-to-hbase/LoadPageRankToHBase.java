package part4.HBase;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.CharacterCodingException;

public class LoadPageRankToHBase {
   public static void main(String[] args) throws IOException {

    	/**************************
       	Initialize HBase Table
      	**************************/
    	// Initialize Config
    	Configuration config = HBaseConfiguration.create();
    	config.set("hbase.master","localhost:60000");
    	HBaseAdmin hbase = new HBaseAdmin(config);

	String tableName = "101062702_pagerank";
    	// Check is table exists. If is, Delete it first
    	if(hbase.tableExists(tableName)){
		hbase.disableTable(tableName);
       		hbase.deleteTable(tableName);
    	}
 
   	// Create a new table(pagerank)
	HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
	tableDescriptor.addFamily(new HColumnDescriptor("Title"));
	tableDescriptor.addFamily(new HColumnDescriptor("PageRank"));
	hbase.createTable(tableDescriptor);

    	/**************************
       	Load inputfile from HDFS
      	**************************/
    
    	// Load input from HDFS
    	Configuration conf = new Configuration();
	HTable pageTable = new HTable(conf, tableName);
    	FileSystem hdfs = FileSystem.get(conf);
    	Path hdfsDir = new Path(args[0]);
    	try {
        	FileStatus[] inputFiles = hdfs.listStatus(hdfsDir);
        	for (int i=0; i<inputFiles.length; i++) {
            		FSDataInputStream fin = hdfs.open(inputFiles[i].getPath());
            		BufferedReader in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
            		String line;
            		//StringBuffer links = new StringBuffer(100);

  			while((line = in.readLine()) != null){
				String[] pageAndRank = getPageAndRank(line);
				System.out.println(pageAndRank[0]+" "+pageAndRank[1]);
				Put put = new Put(Bytes.toBytes(pageAndRank[0]));
				put.add(Bytes.toBytes("PageRank"), Bytes.toBytes("value"), Bytes.toBytes(pageAndRank[1]));
				pageTable.put(put);
            		}
            		in.close();
        	}
    	} catch (IOException e) {
        	e.printStackTrace();
    	}
   }
	private static String[] getPageAndRank(String line) throws CharacterCodingException {
        	String[] pageAndRank = new String[2];
        	int rankIndex = line.indexOf("\t");
        	pageAndRank[1] = line.substring(0, rankIndex);
        	pageAndRank[0] = line.substring(rankIndex+1);
 
        	return pageAndRank;
    	}
 

}
