package calculate-page-rank.Job0;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TotalLinksMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

	private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("Total Links!");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

 	       	String line = value.toString();
		String title = parseTitle(line);
		//ArrayList<String> links = parseLinks(line);
		if(!title.isEmpty())
			output.collect(word, one);
		else 
 			return;
		
		
        }

    

	private String parseTitle(String line){
		String pattern = "(?i)(<title.*?>)(.+?)(</title>)";
		Pattern pTitle = Pattern.compile(pattern);
		Matcher titleMatched = pTitle.matcher(line);
		String title = "";
		while(titleMatched.find()) {
			String tag = titleMatched.group(); 
			title = tag.replaceAll(pattern, "$2");
		}
		return title;
	}
	
	private ArrayList<String> parseLinks(String line){
		String pattern = "(\\[\\[)[\\w\\s]*(\\]\\])";
		Pattern pLink = Pattern.compile(pattern);
		Matcher linkMatched = pLink.matcher(line);
		ArrayList<String> linkList = new ArrayList<String>();
		while(linkMatched.find())
      		{
        		String tag = linkMatched.group();
			String link = tag.replaceAll("(\\[\\[)|(\\]\\])", "");
			if(!linkList.contains(link)){
				linkList.add(link);
			}			
                }
		return linkList;

	}
}
