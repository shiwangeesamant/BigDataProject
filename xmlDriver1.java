import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
*
* @author root
*/
public class xmlDriver1 extends Configured implements Tool {

/**
* @param args the command line arguments
*/
public static void main(String[] args) throws Exception {


int exitCode = ToolRunner.run(new Configuration(),new xmlDriver1(), args);
System.exit(exitCode);
}


public int run(String [] args ) throws IOException, ClassNotFoundException, InterruptedException {

Configuration conf = getConf();

conf.set("xmlinput.start", "<page>");
conf.set("xmlinput.end", "</page>");
conf.set(
"io.serializations",
"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

Job job = new Job(conf, "jobName");


FileInputFormat.setInputPaths(job, args[0]);
job.setJarByClass(xmlDriver1.class);
job.setMapperClass(xmlMapper.class);
job.setNumReduceTasks(0);
job.setInputFormatClass(XmlInputFormat.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
FileOutputFormat.setOutputPath(job, new Path(args[1]));



return job.waitForCompletion(true)?0:1;



}




}
