package exercise1;

// Basic MapReduce utility classes
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
// Classes for File IO
import org.apache.hadoop.fs.Path;
// Wrappers for data types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MrManager extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MrManager(), args);
		System.exit(res);
	}

	// The run method configures and starts the MapReduce job1.
	public int run(String[] args) throws Exception {
		Job Job1 = Job.getInstance(getConf(), "wordcount");
		Job1.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless Job1.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(Job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(Job1, new Path(args[2]));
		Job1.setMapperClass(CountMap.class);
		// Job1.setCombinerClass(CountReduce.class);
		Job1.setReducerClass(CountReduce.class);
		Job1.setOutputKeyClass(Text.class);
		Job1.setOutputValueClass(IntWritable.class);
		Job1.setNumReduceTasks(10);

		if (!Job1.waitForCompletion(true)) {
			System.exit(1);
		}

		Job Job2 = Job.getInstance(getConf(), "stopwords analysis");
		Job2.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless Job1.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(Job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(Job2, new Path(args[1]));
		Job2.setMapperClass(StopwordsMap.class);
		// Job2.setCombinerClass(StopwordsReduce.class);
		Job2.setReducerClass(StopwordsReduce.class);
		Job2.setOutputKeyClass(WordWritable.class);

		Job2.setOutputValueClass(NullWritable.class);
		// Job2.setNumReduceTasks(10);
		Job2.setGroupingComparatorClass(WordGroupingComparator.class);
		Job2.setPartitionerClass(WordPartitioner.class);

		return Job2.waitForCompletion(true) ? 0 : 1;
	}
}

class WordWritable implements Writable, WritableComparable<WordWritable> {

	private Text word;
	private IntWritable freq;

	public WordWritable() {
		this.word = new Text();
		this.freq = new IntWritable();
	}

	public WordWritable(Text word, IntWritable freq) {
		this.word = word;
		this.freq = freq;
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);
		freq.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
		this.freq.readFields(in);

	}

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public IntWritable getFreq() {
		return freq;
	}

	public void setFreq(IntWritable freq) {
		this.freq = freq;
	}

	static WordWritable read(DataInput in) throws IOException {
		WordWritable w = new WordWritable();
		w.readFields(in);
		return w;
	}

	@Override
	public int compareTo(WordWritable o) {
		if (this.freq.get() != o.freq.get()) {
			if (this.freq.get() < o.freq.get()) {
				return 1;
			} else {
				return -1;
			}
		}
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
        final WordWritable other = (WordWritable) obj;

        return this.word.equals(other.word) && this.freq.equals(other.freq);

    }

	@Override
	public String toString() {
		return this.word.toString() + " " + this.freq.get();
	}
}


class WordPartitioner extends Partitioner<WordWritable, NullWritable> {
	@Override
	public int getPartition(WordWritable wordpair, NullWritable nullWritable,
			int numPartitions) {
		return wordpair.getWord().toString().hashCode() % numPartitions;
	}
}

class WordGroupingComparator extends WritableComparator {
	public WordGroupingComparator() {
		super(WordWritable.class, true);
	}

	@Override
	public int compare(WritableComparable tp1, WritableComparable tp2) {
		WordWritable wordPair  = (WordWritable) tp1;
		WordWritable wordPair2 = (WordWritable) tp2;

		return wordPair.getWord().compareTo(wordPair2.getWord());
	}
}
