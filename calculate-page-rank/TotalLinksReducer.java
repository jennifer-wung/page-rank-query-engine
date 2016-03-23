package calculate-page-rank.Job0;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class TotalLinksReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

	@Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
		int sum = 0;
       		while (values.hasNext()) {
         		sum += values.next().get();
       		}
       		output.collect(key, new IntWritable(sum));
	}
}

