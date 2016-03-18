package part3;

import java.io.IOException;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class IndexReducer extends MapReduceBase implements Reducer<Text,Text,Text,DocSumWritable> {

        private TreeMap<String, Integer> map;
        private TreeMap<String, Integer> tempTreeMap;

        private void add(String tag, Integer tVal) {
                Integer val;

                if (map.get(tag) != null) {
                        val = map.get(tag);
                        map.remove(tag);
                }else {
                        val = 0;
                }

                map.put(tag, val+tVal);
        }

        private TreeMap<String, Integer> convertToString2TreeMap(String text){
                TreeMap<String, Integer> data = new TreeMap<String, Integer>();
                //Pattern p = Pattern.compile("[\\{\\}\\=\\,]++");
		String tpattern = "(?i)(<title.*?>)(.+?)(</title>)";
		Pattern pTitle = Pattern.compile(tpattern);
		String dpattern = "(?i)(<docfreq>)(.+?)(</docfreq>)";
		Pattern pDocfreq = Pattern.compile(dpattern);
		Matcher titleMatched = pTitle.matcher(text);
		Matcher dfMatched = pDocfreq.matcher(text);

		while(titleMatched.find() && dfMatched.find()) {
			String title = "";
			String tag = titleMatched.group(); 
			title = tag.replaceAll(tpattern, "$2");
			if(tag.charAt(0)==' ')
                                title = title.substring(1);
			
			String df = "";
			String dfTag = dfMatched.group();
                        df = dfTag.replaceAll("[^0-9]", "");
			data.put(title, Integer.valueOf(df));
		}
                /*String[] split = p.split(text);
                for ( int i=1; i+2 <= split.length; i+=2 ){	
			String doc = new String();
			if(split[i].charAt(0)==' ')
				doc = split[i].substring(1);
			else 
  				doc = split[i];
                        data.put(doc, Integer.valueOf(split[i+1]) );
                }*/
                return data;
        }

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DocSumWritable> output, Reporter reporter)
            throws IOException {
                map = new TreeMap<String, Integer>();

                while(values.hasNext()){
                        tempTreeMap = new TreeMap<String, Integer>();
			String mapStr = values.next().toString();
			tempTreeMap = convertToString2TreeMap(mapStr);
                        for(String tag : tempTreeMap.keySet()){
                                add(tag, tempTreeMap.get(tag));
                        }
                }

		//map = new HashMap<String, Integer>();

	        //while(values.hasNext()){
	        //    add(values.next().toString());
	        //}
                output.collect(key, new DocSumWritable(map));

    }
}


