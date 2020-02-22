
     import java.io.IOException;  
   import java.util.StringTokenizer;  
   import org.apache.hadoop.io.IntWritable;  
   import org.apache.hadoop.io.LongWritable;  
   import org.apache.hadoop.io.Text;  
   import org.apache.hadoop.mapred.MapReduceBase;  
   import org.apache.hadoop.mapred.Mapper;  
   import org.apache.hadoop.mapred.OutputCollector;  
   import org.apache.hadoop.mapred.Reporter;  
   public class Wcp_mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{    
      public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,   
              Reporter reporter) throws IOException{  
          String line = value.toString();  // used to convert text class to string 
         StringTokenizer  tokenizer = new StringTokenizer(line); // class used to divide into tokens 
          while (tokenizer.hasMoreTokens()){  
              String p=(tokenizer.nextToken());//string will be divided into tokens and stored in the object p  
              output.collect(new Text(p), new IntWritable(1));  // used to collect the data in the form of Text and IntWritable(ex:hi,1)
           }  
       }  
    
  }  