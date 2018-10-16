package exercise1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class StopwordsReduce extends
	Reducer<WordWritable, NullWritable, WordWritable, NullWritable> {

		private static final Logger LOGGER = LogManager.getRootLogger();
		private int cnt = 0;
		private static int count = 0;

		@Override
		public void reduce(WordWritable word, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path filenamePath = new Path("/user/vagrant/stopwords/stopwords" + (cnt++) + ".csv");
			FSDataOutputStream out;
	//		if (!fs.exists(filenamePath)) {
				try {
					out = fs.create(filenamePath);
					if (word.getFreq().get() > 4000)
						out.writeBytes(word.getWord().toString() + "\n");
					out.close();
					count++;
					if (count <= 10)
						context.write(word, NullWritable.get());
				} catch (Exception e) {
				}

	//		}
		}

		@Override
		protected void cleanup(Context output){
			output.getCounter("RecordCounter","Reducer-no-"+output.getConfiguration().getInt("mapreduce.task.partition",
		                    0)).increment(count);
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
	public String toString() {
		return this.word.toString() + " " + this.freq.get();
	}
}
