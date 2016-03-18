package part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocSumWritable implements Writable{

        private TreeMap<String,Integer> map = new TreeMap<String, Integer>();

        public DocSumWritable() {
        }

        public DocSumWritable(TreeMap<String,Integer> map){

                this.map = map;
        }

        private Integer getCount(String tag){
                return map.get(tag);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
                Iterator<String> it = map.keySet().iterator();
                Text tag = new Text();

                while(it.hasNext()){
                        String t = it.next();
                        tag = new Text(t);
                        tag.readFields(in);
                        new IntWritable(getCount(t)).readFields(in);
                }

        }

        @Override
        public void write(DataOutput out) throws IOException {
                Iterator<String> it = map.keySet().iterator();
                Text tag = new Text();
                IntWritable count = new IntWritable();

                while(it.hasNext()){
                        String t = it.next();
                        new Text(t).write(out);
			new IntWritable(getCount(t)).write(out);
                }

        }

        @Override
        public String toString() {

                String output = "";
                Integer docFreq = map.size();

                output += docFreq.toString() + "\t";
                for(String tag : map.keySet()){
                        output += (tag+"="+getCount(tag).toString()+"]");
                }

                return output;

        }

}

