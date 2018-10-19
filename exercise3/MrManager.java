package exercise3;

// Basic MapReduce utility classes
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configured;
// Classes for File IO
import org.apache.hadoop.fs.Path;
// Wrappers for data types
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// Configurable counters
class DocumentWritable implements Writable , Serializable {

	private String documentName;
	private int freq;

	public DocumentWritable () {
		
	}
	public DocumentWritable(String documentName, int freq) {
		this.documentName = documentName;
		this.freq = freq;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.documentName + '#');
		out.writeInt(freq);
	}

	public void readFields(DataInput in) throws IOException {
		this.documentName = in.readUTF();
		this.freq = in.readInt();
	}

	public String getDocumentName() {
		return documentName;
	}

	public void setDocumentName(String documentName) {
		this.documentName = documentName;
	}

	public int getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}
	
	public String toString() {
		return this.documentName + '#' + this.freq;
	}
}
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
		Job1.setOutputValueClass(DocumentWritable.class);
		Job1.addCacheFile(new Path(args[3]).toUri());
		if (!Job1.waitForCompletion(true)) {
			System.exit(1);
		}
//
		Job Job2 = Job.getInstance(getConf(), "stopwords analysis");
		Job2.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless Job1.setInputFormatClass is used
		FileInputFormat.addInputPath(Job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(Job2, new Path(args[1]));
		Job2.setMapperClass(IndexerMap.class);
		Job2.setCombinerClass(IndexerReduce.class);
		Job2.setReducerClass(IndexerReduce.class);
		Job2.setOutputValueClass(Text.class);
		Job2.setOutputKeyClass(Text.class);
		return Job2.waitForCompletion(true) ? 0 : 1;
  }
}
