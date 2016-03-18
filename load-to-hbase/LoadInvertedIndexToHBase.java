package part4.HBase;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.LinkedHashMap;

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

public class LoadInvertedIndexToHBase {
   public static void main(String[] args) throws IOException {

    	/**************************
       	Initialize HBase Table
      	**************************/
    	// Initialize Config
    	Configuration config = HBaseConfiguration.create();
    	config.set("hbase.master","localhost:60000");
    	HBaseAdmin hbase = new HBaseAdmin(config);

	String tableName = "101062702_invertedIndex";
    	// Check is table exists. If is, Delete it first
    	if(hbase.tableExists(tableName)){
		hbase.disableTable(tableName);
       		hbase.deleteTable(tableName);
    	}
 
   	// Create a new table(pagerank)
	HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
	tableDescriptor.addFamily(new HColumnDescriptor("Term"));
	tableDescriptor.addFamily(new HColumnDescriptor("DocFreq"));
	tableDescriptor.addFamily(new HColumnDescriptor("Document"));
	tableDescriptor.addFamily(new HColumnDescriptor("TermFreq"));
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

  			while((line = in.readLine()) != null){
				int termIndex = line.indexOf("\t");
				int dfIndex = line.indexOf("\t",termIndex+1);

				String term = line.substring(0, termIndex);
				String docFreq = line.substring(termIndex+1, dfIndex);
				LinkedHashMap<String, String> docInfoMap = getDocInfo(line.substring(dfIndex+1));
				//System.out.println(pageAndRank[0]+" "+pageAndRank[1]);
				
				Put put = new Put(Bytes.toBytes(term));
				put.add(Bytes.toBytes("DocFreq"), Bytes.toBytes("df"), Bytes.toBytes(docFreq));
				int id = 1;
				for(String doc : docInfoMap.keySet()){
					String docId = "doc"+id;
					put.add(Bytes.toBytes("Document"), Bytes.toBytes(docId), Bytes.toBytes(doc));
					put.add(Bytes.toBytes("TermFreq"), Bytes.toBytes(docId), Bytes.toBytes(docInfoMap.get(doc)));
					id++;
				}
				pageTable.put(put);
            		}
            		in.close();
        	}
    	} catch (IOException e) {
        	e.printStackTrace();
    	}
   }
	private static LinkedHashMap<String, String> getDocInfo(String line) throws CharacterCodingException {
 		LinkedHashMap<String, String> docInfoMap = new LinkedHashMap<String, String>();
		//String tPattern = "(?i)(<title.*?>)(.+?)(</title>)";
		/*String dfPatter = "(?i)(</title>=)(.+?)(,)";
                Pattern pTitle = Pattern.compile(tPattern);
		Pattern pDF = Pattern.compile(dfPattern);
		Matcher titleMatched = pTitle.matcher(line);
		Matcher dfMatched = pDF.matcher(line);
                while(titleMatched.find() && dfMatched.find()) {
			String title = "";
			String tag = titleMatched.group(); 
			title = tag.replaceAll(tpattern, "$2");
			
			String df = "";
			String dfTag = dfMatched.group();
                        df = dfTag.replaceAll("[^0-9]", "");
			data.put(title, df);
		}*/
		Pattern p = Pattern.compile("[\\=\\]]++");
		String[] docAndtf = p.split(line);
		for ( int i=0; i+2 <= docAndtf.length; i+=2 ){
                        docInfoMap.put(docAndtf[i], docAndtf[i+1]);
                }
        	return docInfoMap;
    	}
 

}
