package part3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class InvertedIndexBasic {

        public static void main(String[] args) throws Exception {
                JobConf conf = new JobConf(InvertedIndexBasic.class);
                conf.setJobName("HW2_part3");
			
		conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(Text.class);
                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(DocSumWritable.class);
		
                conf.setMapperClass(IndexMapper.class);
                conf.setCombinerClass(IndexCombiner.class);
                conf.setReducerClass(IndexReducer.class);
		
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(args[0]));
             	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

                JobClient.runJob(conf);
        }
}

