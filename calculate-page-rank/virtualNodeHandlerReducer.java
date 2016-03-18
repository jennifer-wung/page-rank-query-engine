package part2.Job1_5;

import part2.Job1_5.virtualNodeHandlerMapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;


public class virtualNodeHandlerReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
            	String pageRank = "1.0\t";
		boolean first = true;
		while (values.hasNext()) {
			if(!first) pageRank += ",";
			
			pageRank += (values.next().toString());
			first = false;
                }
        	output.collect(key, new Text(pageRank));			

        }

}
		

