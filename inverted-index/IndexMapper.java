package part3;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class IndexMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

	String pattern = "(?i)(<text.*?>)(.+?)(</text>)";
	Pattern pText = Pattern.compile(pattern);
	@Override
    	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        	String line = value.toString();
		String title = parseTitle(line);

		/*******parse words in the <text> </text> tag***/
		String text = "";
		Matcher textMatched = pText.matcher(line);
		while(textMatched.find()) {
			String tag = textMatched.group(); 
			text = tag.replaceAll(pattern, "$2");
		}
		//if(text.compareTo("#REDIRECT  {{R from CamelCase}}")==0)
		//	return;
	
		StringTokenizer tokenizer = new StringTokenizer(text);
        	while(tokenizer.hasMoreTokens()){
                	String token = tokenizer.nextToken();
                	String[] parts = token.split("\\W");
            		for(int ii=0; ii<parts.length;ii+=1){
               			String isAlphaToken = parts[ii].replaceAll("[^A-Za-z]+", "");
					if(isAlphaToken.length()!=0)
						output.collect(new Text(isAlphaToken.toLowerCase()), new Text(title));
			}
        	}

    	}	

	private String parseTitle(String line){
		String pattern = "(?i)(<title.*?>)(.+?)(</title>)";
		Pattern pTitle = Pattern.compile(pattern);
		Matcher titleMatched = pTitle.matcher(line);
		String title = "";
		while(titleMatched.find()) {
			String tag = titleMatched.group(); 
			title = tag;//.replaceAll(pattern, "$2");
		}
		return title;
	}
}

