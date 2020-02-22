
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WriteToHdfs {
	public static void main(String[] args) throws IOException 
	{
	Configuration conf = new Configuration();
	String localpath="/home/hadoop/Desktop/WRITE";
	String uri= "hdfs://localhost:8020";
	String Hdfspath="hdfs://localhost:8020/srk";
	FileSystem fs=FileSystem.get(URI.create(uri),conf);
	fs.copyFromLocalFile(new Path(localpath),new Path(Hdfspath));
	}

}
