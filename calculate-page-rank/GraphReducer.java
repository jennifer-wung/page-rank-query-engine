package calculate-page-rank.Job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;


public class GraphReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	@Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
		String pageRank = "1.0\t";
		boolean first = true;
		while (values.hasNext()) {
			if(!first) pageRank += ",";
			
			pageRank += (values.next().toString());
			if(pageRank.compareTo("1.0\t")==0){
				output.collect(key, new Text(pageRank+"]"));
                        	return;
			} else {
				first = false;
			}
                }
			output.collect(key, new Text(pageRank));
	}
}
