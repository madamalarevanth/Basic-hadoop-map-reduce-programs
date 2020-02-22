
import java.io.IOException;  
import java.util.Iterator;  
  import org.apache.hadoop.io.IntWritable;  
  import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapred.MapReduceBase;  
import org.apache.hadoop.mapred.OutputCollector;  
import org.apache.hadoop.mapred.Reducer;  
 import org.apache.hadoop.mapred.Reporter;  
 
public class WordCountReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {  
public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,  
  Reporter reporter) throws IOException {  
 int sum=0;  
while (values.hasNext()) {  //(hi,[1,1,1,1]) is the input value to iterator and 
	// values.hasnext is used to verify whether data is existing or not its a boolean statement 
 sum+=values.next().get();// sum=sum+values.next() is used to take the value 1 from the iterator 
 // but it is a intwritable data .so u need to convert into integer becouse sum is of int type so we use a method 
 // .get() for convertion   
 
 }  
output.collect(key,new IntWritable(sum));  
 }  
 }  
