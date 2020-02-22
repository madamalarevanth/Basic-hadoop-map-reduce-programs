import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * MaxClosePriceMapper.java
 * This is a Mapper program to calculate Max Close Price from stock dataset using MapReduce
 */

public class MaxClosePriceMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable>{  
	   
    public void map(LongWritable key, Text value,OutputCollector<Text,FloatWritable> output,   
            Reporter reporter) throws IOException{    
	
	//public void map(LongWritable key, Text value, Context context)
			//throws IOException, InterruptedException {

		String line = value.toString();
		String[] items = line.split(",");
		
		String stock = items[1];
		Float closePrice = Float.parseFloat(items[6]);
		
		output.collect(new Text(stock), new FloatWritable(closePrice));
		
	}
}  
   
