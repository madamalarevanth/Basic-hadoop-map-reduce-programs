
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class WordCount{  
public static void main(String[] args) throws IOException{  
JobConf conf = new JobConf(WordCount.class);        // defining job configuration
conf.setJobName("WordCount1"); // used to give name to the job
	conf.setInputFormat(TextInputFormat.class);  // take input from the file in text format
      conf.setOutputFormat(TextOutputFormat.class); // take output from the file in text format
        conf.setOutputKeyClass(Text.class);  // output key format
       conf.setOutputValueClass(IntWritable.class);          // output value format
      conf.setMapperClass(WordCountMapper.class);  
      conf.setCombinerClass(WordCountReducer.class);  
      conf.setReducerClass(WordCountReducer.class);           
       FileInputFormat.setInputPaths(conf,new Path(args[0]));  // a.txt file (input file)
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));   //output directory out
       JobClient.runJob(conf); // used to start the job 
    }  
}  
