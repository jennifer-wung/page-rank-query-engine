package inverted-index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class IndexCombiner extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

        private HashMap<String, String> map;
	String pattern = "(?i)(<docfreq>)(.+?)(</docfreq>)";
        private void add(String key) {
                Integer val;

                if (map.get(key) != null) {
                        String dfStr = map.get(key);
			Pattern pDF = Pattern.compile(pattern);
			Matcher dfMatched = pDF.matcher(dfStr);
			String df = "";
			while(dfMatched.find()) {
				String tag = dfMatched.group(); 
				df = tag.replaceAll(pattern, "$2");
			}
			val = Integer.valueOf(df);
                        map.remove(key);
                } else {
                        val = 0;
                }
		//Integer totalVal = val+1;
		String valStr = "<docfreq>"+(val+1)+"</docfreq>"; 
                map.put(key, valStr);
        }

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
                map = new HashMap<String, String>();

                while(values.hasNext()){
                        add(values.next().toString());
                }

                String hashMapStr = map.toString();

                output.collect(key, new Text(hashMapStr));

    }
}

