

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class partioner implements Partitioner<Text,IntWritable> {

	public int getPartition(Text key, IntWritable value, int argc) {
		String p=key.toString();//hi
		if(p.length()==2) // we will verify the length of the text if it is 2 we will give the words of length 2 to the reducer 1
			return 0;  //return 0 indicates 1st reducer
		else if(p.length()>3) // we will verify the length of the text if it is >3 we will give the words of length >3 to the reducer 2
			return 1;//return 0 indicates 2nd reducer
		else 
			return 2;// reducer 3 
	}

	@Override
	public void configure(JobConf arg0) {
}
}