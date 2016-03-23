package build-graph;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class GraphJob {

        public static void main(String[] args) throws Exception {
                JobConf conf = new JobConf(GraphJob.class);
                conf.setJobName("build-graph");

                conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(Text.class);
                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(Text.class);

                conf.setMapperClass(GraphMapper.class);
                conf.setReducerClass(GraphReducer.class);

                conf.setNumReduceTasks(1);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(args[0]));
                FileOutputFormat.setOutputPath(conf, new Path(args[1]));

                JobClient.runJob(conf);
        }
}
