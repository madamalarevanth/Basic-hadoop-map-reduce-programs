 import java.io.IOException;  
 import java.util.Iterator;  
   import org.apache.hadoop.io.FloatWritable;  
   import org.apache.hadoop.io.Text;  
 import org.apache.hadoop.mapred.MapReduceBase;  
 import org.apache.hadoop.mapred.OutputCollector;  
 import org.apache.hadoop.mapred.Reducer;  
  import org.apache.hadoop.mapred.Reporter;



/**
 * MaxClosePriceReducer.java
 * www.hadoopinrealworld.com
 * This is a Reduce program to calculate Max Close Price from stock dataset using MapReduce
 */
  public class MaxClosePriceReducer extends MapReduceBase implements Reducer<Text,FloatWritable,Text,FloatWritable> {  
	  public void reduce(Text key, Iterator<FloatWritable> values,OutputCollector<Text,FloatWritable> output,  
	     Reporter reporter) throws IOException {  

		 float maxClosePrice = Float.MIN_VALUE;
		 
		 
		 while (values.hasNext()) {  
			 maxClosePrice = Math.max(maxClosePrice, values.next().get());
			   }  
		 
		 
		 //Iterate all temperatures for a year and calculate maximum
		
		 
		 //Write output
		output.collect(key, new FloatWritable(maxClosePrice));
	 }
 }
 
