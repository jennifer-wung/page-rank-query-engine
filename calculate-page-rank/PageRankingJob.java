package calculate-page-rank;

import calculate-page-rank.Job0.TotalLinksMapper;
import calculate-page-rank.Job0.TotalLinksReducer;
import calculate-page-rank.Job1.GraphMapper;
import calculate-page-rank.Job1.GraphReducer;
import calculate-page-rank.Job1_5.virtualNodeHandlerMapper;
import calculate-page-rank.Job1_5.virtualNodeHandlerReducer;
import calculate-page-rank.Job2.PageRankMapper;
import calculate-page-rank.Job2.PageRankReducer;
import calculate-page-rank.Job3.RankingMapper;
import calculate-page-rank.Job3.RankingKeyComparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRankingJob {
	private static NumberFormat nf = new DecimalFormat("00");
	public static int totalLinksCount = 0;

        public static void main(String[] args) throws Exception {
		PrintWriter writer = new PrintWriter("convergeRate.txt", "UTF-8");	
		PageRankingJob pageRanking = new PageRankingJob();
		
		//Job0: Count Total Links
		pageRanking.runCountTotalLinks(args[0], args[1]);//mapreduce
		totalLinksCount = getTotalLinks(args[1]+"/part-00000");
		
		//Job1: parse XML
		String outputPath = args[1]+"/ranking/iter";
		pageRanking.runXmlParsing(args[0], args[1]+"/parsing");//mapreduce
		
		pageRanking.runVirtualNodeHandler(args[1]+"/parsing", outputPath+nf.format(0));//mapreduce

		int runs = 0;
        	for (; runs < 75; runs++) {
            		//Job 2: Calculate new rank
            		pageRanking.runRankCalculation(outputPath+nf.format(runs), outputPath+nf.format(runs+1));
			rankingSumAndWrite(outputPath+nf.format(runs+1)+"/part-00000",writer);
			System.out.println("iter = "+(runs+1));
        	}

		//Job 3: Order by rank
        	pageRanking.runRankOrdering(outputPath+nf.format(runs), args[1]+"/result");
		writer.close();

        }
	public void runCountTotalLinks(String inputPath, String outputPath) throws IOException {
                JobConf conf = new JobConf(PageRankingJob.class);

                // Input / Mapper
                FileInputFormat.setInputPaths(conf, new Path(inputPath));
                conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(IntWritable.class);
		conf.setInputFormat(TextInputFormat.class);
                conf.setMapperClass(TotalLinksMapper.class);

                // Output / Reducer
                FileOutputFormat.setOutputPath(conf, new Path(outputPath));
                conf.setOutputFormat(TextOutputFormat.class);
                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(IntWritable.class);
                conf.setReducerClass(TotalLinksReducer.class);

                JobClient.runJob(conf);
        }

	public void runXmlParsing(String inputPath, String outputPath) throws IOException {
	    	JobConf conf = new JobConf(PageRankingJob.class); 		

        	// Input / Mapper
        	FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(Text.class);
        	conf.setInputFormat(TextInputFormat.class);
        	conf.setMapperClass(GraphMapper.class);
 
        	// Output / Reducer
        	FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        	conf.setOutputFormat(TextOutputFormat.class);
        	conf.setOutputKeyClass(Text.class);
        	conf.setOutputValueClass(Text.class);
        	conf.setReducerClass(GraphReducer.class);
 
        	JobClient.runJob(conf);
    	}

	public void runVirtualNodeHandler(String inputPath, String outputPath) throws IOException {
                JobConf conf = new JobConf(PageRankingJob.class);

                // Input / Mapper
                FileInputFormat.setInputPaths(conf, new Path(inputPath));
                conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(Text.class);
                conf.setInputFormat(TextInputFormat.class);
                conf.setMapperClass(virtualNodeHandlerMapper.class);

                // Output / Reducer
                FileOutputFormat.setOutputPath(conf, new Path(outputPath));
                conf.setOutputFormat(TextOutputFormat.class);
                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(Text.class);
                conf.setReducerClass(virtualNodeHandlerReducer.class);

                JobClient.runJob(conf);
        }

 
    	private void runRankCalculation(String inputPath, String outputPath) throws IOException {
        	JobConf conf = new JobConf(PageRankingJob.class);
 		
		System.out.println("totalLinks="+String.valueOf(totalLinksCount));
		conf.set("totalLinksCount",String.valueOf(totalLinksCount));
        	conf.setOutputKeyClass(Text.class);
        	conf.setOutputValueClass(Text.class);
 
        	conf.setInputFormat(TextInputFormat.class);
        	conf.setOutputFormat(TextOutputFormat.class);
 
        	FileInputFormat.setInputPaths(conf, new Path(inputPath));
        	FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        	conf.setMapperClass(PageRankMapper.class);
        	conf.setReducerClass(PageRankReducer.class);
 
        	JobClient.runJob(conf);
    	}

	private void runRankOrdering(String inputPath, String outputPath) throws IOException {
     		JobConf conf = new JobConf(PageRankingJob.class);
 
        	conf.setOutputKeyClass(FloatWritable.class);
        	conf.setOutputValueClass(Text.class);
        	conf.setInputFormat(TextInputFormat.class);
        	conf.setOutputFormat(TextOutputFormat.class);
 
        	FileInputFormat.setInputPaths(conf, new Path(inputPath));
        	FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        	conf.setMapperClass(RankingMapper.class);
 		conf.setOutputKeyComparatorClass(RankingKeyComparator.class);
        	JobClient.runJob(conf);
    	}

	private static int getTotalLinks(String filePath){
		try{
			Path ofile = new Path(filePath);
	                FileSystem fs = ofile.getFileSystem(new Configuration());
        	        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(ofile)));
			String line = reader.readLine();
			Pattern p = Pattern.compile("\\t");
			String[] split = p.split(line);
        		String numStr = split[1];
			return Integer.parseInt(numStr);
		}catch(IOException e){
			System.out.println("Unable to read from file.");
			return 1;
		}
	}
	
	private static void rankingSumAndWrite(String filePath, PrintWriter writer) throws IOException {
		float rankingSum = 0.0F;
		Path file = new Path(filePath);
               	FileSystem fs = file.getFileSystem(new Configuration());
               	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
               	String line = reader.readLine();
		while(line != null){
			int titleTabIndex = line.indexOf("\t");
			int rankTabIndex = line.indexOf("\t",titleTabIndex+1);
			String title = line.substring(0,titleTabIndex);
			String rank = line.substring(titleTabIndex+1,rankTabIndex);
			Float rankfloat = Float.valueOf(rank);
			if(title.compareTo("]")!=0)
				rankingSum += rankfloat;
			line = reader.readLine();
		}
		writer.println(rankingSum);
		

	}


}
