package calculate-page-rank.Job2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PageRankMapper  extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
		// example value to text: Page_B 1.0 Page_A 
		int titleTabIndex = value.find("\t");
        	int rankTabIndex = value.find("\t", titleTabIndex+1);
		//int otherPageIndex = value.find(",",rankTabIndex+1);
		
		String title = Text.decode(value.getBytes(), 0, titleTabIndex);
        	String titleWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
		
		// Mark page as an Existing page (ignore red wiki-links)
        	output.collect(new Text(title), new Text("!"));
		// Skip pages with no links.
        	if(rankTabIndex == -1) return;

		String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
		String[] allOtherPages = links.split(",");
        	int totalLinks = allOtherPages.length;

                // Skip pages with no links.
                if(rankTabIndex == -1) return;

		for (String otherPage : allOtherPages){
	            	Text titleRankTotalLinks = new Text(titleWithRank + totalLinks);
            		output.collect(new Text(otherPage), titleRankTotalLinks);
        	}
 
        	// Put the original links of the page for the reduce output
        	output.collect(new Text(title), new Text("|"+links));
	}

}


