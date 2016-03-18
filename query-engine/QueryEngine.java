package part5;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.Collections;

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
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;

import java.nio.charset.CharacterCodingException;

public class QueryEngine {
   public static void main(String[] args) throws IOException {
	
	Map<String,Double> docIdAndtwMap = new LinkedHashMap<String, Double>();
	Map<String,Double> docIdAndRankMap = new LinkedHashMap<String, Double>();
	String pageTableName = "101062702_pagerank";
        String invertedTableName = "101062702_invertedIndex";
	String queryTerm = new String();
	String totalPagesCountPath = new String();
	int rowCount;
	/**************
        Get user's query term
        ***************/
	if (args.length == 2) {
		totalPagesCountPath = args[0];
    		queryTerm = args[1];
	} else {
		System.out.println("You need to give me someting!");
		System.exit(1);
	}

    	/****************************************************
       	Get two tables from HBase and retrieve values in them
      	******************************************************/
    	// Initialize Config
    	Configuration config = HBaseConfiguration.create();
    	config.set("hbase.master","localhost:60000");
    	HBaseAdmin hbase = new HBaseAdmin(config);

    	// Check is table exists. If is, Delete it first
    	if(hbase.tableExists(pageTableName) && hbase.tableExists(invertedTableName)){
	   try{
		Configuration conf = new Configuration();
        	HTable pageTable = new HTable(conf, pageTableName);	
		HTable invertedTable = new HTable(conf, invertedTableName);

		FileSystem hdfs = FileSystem.get(conf);
                Path hdfsDir = new Path(totalPagesCountPath);
		FileStatus[] inputFiles = hdfs.listStatus(hdfsDir);
		FSDataInputStream fin = hdfs.open(inputFiles[0].getPath());
		BufferedReader in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		String line = in.readLine();
		int index = line.indexOf("\t");	
		rowCount = Integer.parseInt(line.substring(index+1));
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+rowCount);
        	//rowCount = 85;//(int)aggregationClient.rowCount(Bytes.toBytes(pageTableName), null, scan);//total # of documents
		docIdAndtwMap = getDocIdAndTermFreq(invertedTable, queryTerm, rowCount);
		docIdAndRankMap = retrieveDocAndRank(pageTable,docIdAndtwMap);
		print(docIdAndRankMap);
		
	   } catch (Throwable e) {
                e.printStackTrace();
           }
	
    	} else {
		if (!hbase.tableExists(pageTableName))
			System.out.println("Page Rank Table does not exit in Hbase.");
		else if (hbase.tableExists(invertedTableName))
			System.out.println("Inverted Index Table does not exit in Hbase.");
		else 
			System.out.println("Both Tables are not in Hbase.");
		System.exit(1);
	}

   }


   /*******************functions*****************************/


	/******************************************
        Get query's document names and term weights
        ********************************************/
	private static Map<String, Double> getDocIdAndTermFreq(HTable invertedTable, String query, int docCount) throws IOException {
		Map<String,Double> docIdAndtw = new LinkedHashMap<String, Double>();
 		Configuration conf = new Configuration();
		Get get = new Get(Bytes.toBytes(query));
		get.addFamily(Bytes.toBytes("DocFreq"));
		Result result = invertedTable.get(get);
		Integer df = Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("DocFreq"), Bytes.toBytes("df"))));
		
		get.addFamily(Bytes.toBytes("Document"));
		Result docResult = invertedTable.get(get);
		
		get.addFamily(Bytes.toBytes("TermFreq"));
		Result tfResult = invertedTable.get(get);
		
		for (int i = 1; i <= df; i++) {
                        String docId = "doc"+i;
                        String docName = Bytes.toString(docResult.getValue(Bytes.toBytes("Document"), Bytes.toBytes(docId)));
                        Integer tf = Integer.valueOf(Bytes.toString(tfResult.getValue(Bytes.toBytes("TermFreq"), Bytes.toBytes(docId))));
			double w = 0.0;
                       	if (tf> 0) {
                    		 w = tf;
                      	}
                 	Double weightValue = w * Math.log((double)docCount/df);
                        docIdAndtw.put(docName, weightValue);
                }

        	return docIdAndtw;
    	}

	/*********************************************
        Get page ranking value and use ranking policy 
        ***********************************************/
 	private static Map<String, Double> retrieveDocAndRank(HTable pageTable, Map<String, Double> docIdAndtw) throws IOException {
		Map<String,Double> queryPageRank = new LinkedHashMap<String, Double>();
		Configuration conf = new Configuration();
		for (String doc : docIdAndtw.keySet()) {
			Get get = new Get(Bytes.toBytes(doc));
			get.addFamily(Bytes.toBytes("PageRank"));
                	Result result = pageTable.get(get);
			Double pr = Double.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("PageRank"), Bytes.toBytes("value"))));
			Double tw = docIdAndtw.get(doc);
			Double finalPR = pr*0.5 + tw*0.5;//ranking policy = PageRank*0.5 + TermWeighting*0.5
			queryPageRank.put(doc, finalPR);
			}
		return queryPageRank;
	}
	
	private static void print(Map<String, Double> docIdAndRankMap) throws IOException {
		Map<String, Double> sortedRankingMap = sortByValues(docIdAndRankMap);
		Iterator it = sortedRankingMap.entrySet().iterator();

		System.out.println("Pages are:\n");
		int i = 1;
    		while (it.hasNext()) {
        		Map.Entry pairs = (Map.Entry)it.next();
        		System.out.println(i+". "+pairs.getKey()+" = "+pairs.getValue() + "(ranking value)");
        		it.remove(); // avoids a ConcurrentModificationException
			i++;
    		}
	}

	private static Map<String, Double> sortByValues(Map<String,Double> map) {
                List list = new LinkedList(map.entrySet());
                // Defined Custom Comparator here
                Collections.sort(list, new Comparator() {
                        public int compare(Object o1, Object o2) {
                                return -((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
                        }
                });

                // Here I am copying the sorted list in HashMap
                // using LinkedHashMap to preserve the insertion order
                Map<String, Double> sortedHashMap = new LinkedHashMap<String, Double>();
                for (Iterator it = list.iterator(); it.hasNext();) {
                        Map.Entry entry = (Map.Entry) it.next();
                        sortedHashMap.put((String)entry.getKey(),(Double)entry.getValue());
                }
                return sortedHashMap;
        }
}
