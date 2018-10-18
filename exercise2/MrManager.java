package excercise2;

// Basic MapReduce utility classes
import org.apache.hadoop.conf.Configured;
// Classes for File IO
import org.apache.hadoop.fs.Path;
// Wrappers for data types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// Configurable counters

public class MrManager extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MrManager(), args);
    System.exit(res);
  }

// The run method configures and starts the MapReduce Job1.

	public int run(String[] args) throws Exception {
		 Job Job1 = Job.getInstance(getConf(), "wordcount");
		Job1.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless Job1.setInputFormatClass is used
		FileInputFormat.addInputPath(Job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(Job1, new Path(args[2]));
		Job1.setMapperClass(UniqueTermsMap.class);
		Job1.setCombinerClass(UniqueTermsReduce.class);
		Job1.setReducerClass(UniqueTermsReduce.class);
		Job1.setOutputKeyClass(Text.class);
		Job1.setOutputValueClass(Text.class);
		Job1.addCacheFile(new Path(args[3]).toUri());

		return Job1.waitForCompletion(true) ? 0 : 1;
  }
}

