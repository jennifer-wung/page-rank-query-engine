package build-graph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;


public class GraphReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	Text value = new Text();
	@Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
		String pageRank = "1.0\t";
		while (values.hasNext()) {
                        value = (Text) values.next();
			pageRank += (value.toString()+",");
                }

		output.collect(key, new Text(pageRank));
	}

}
