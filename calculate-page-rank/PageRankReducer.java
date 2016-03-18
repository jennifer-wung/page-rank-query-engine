package part2.Job2;

import part2.Job1.GraphReducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;


public class PageRankReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private static final float damping = 0.85F;
	private float totalLinks = 0.0F;

	public void configure(JobConf job){
		totalLinks = Float.parseFloat(job.get("totalLinksCount"));
	}

        @Override
        public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
                boolean isExistingPage = false;
		String[] split;
		float sumShareOtherPageRanks = 0;
		String links = "";
		String pageWithRank;

		// For each otherPage: 
        	// - check control characters
        	// - calculate pageRank share <rank> / count(<links>)
        	// - add the share to sumShareOtherPageRanks
        	while(values.hasNext()){
            		pageWithRank = values.next().toString();
            
            		if(pageWithRank.equals("!")) {
                		isExistingPage = true;
                		continue;
            		}
            
            		if(pageWithRank.startsWith("|")){
                		links = "\t"+pageWithRank.substring(1);
                		continue;
            		}

            		split = pageWithRank.split("\\t");
            
            		float pageRank = Float.valueOf(split[1]);
            		int countOutLinks = Integer.valueOf(split[2]);
            
            		sumShareOtherPageRanks += (pageRank/countOutLinks);
        	}	
        	if(!isExistingPage) return;
        	float newRank = damping * sumShareOtherPageRanks + (1-damping)/totalLinks;

        	output.collect(page, new Text(newRank + links));			

        }

}
		
