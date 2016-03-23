package calculate-page-rank.Job3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class RankingKeyComparator extends WritableComparator{

        protected RankingKeyComparator() {
                super(FloatWritable.class,true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
                FloatWritable t1 = (FloatWritable)w1;
                FloatWritable t2 = (FloatWritable)w2;
                Float num1 = Float.parseFloat(t1.toString());
                Float num2 = Float.parseFloat(t2.toString());
                return -(num1.compareTo(num2));
        }
}
