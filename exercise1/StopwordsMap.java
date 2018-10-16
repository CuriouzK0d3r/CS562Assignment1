package exercise1;

// Basic Java file IO
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Regular expression utility
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StopwordsMap extends
		Mapper<LongWritable, Text, WordWritable, NullWritable> {

	// Word boundary defined as whitespace-characters-word boundary-whitespace
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private static final Logger LOGGER = LogManager.getRootLogger();
	private static final Log LOG = LogFactory.getLog(StopwordsMap.class);

	protected void setup(
			Mapper<LongWritable, Text, WordWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {

	}

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {

		String line = lineText.toString();
		String currentWord = "";

		int cnt = 0;

		for (String word : WORD_BOUNDARY.split(line)) {
			if (word.isEmpty()) {
				continue;
			}
			if (cnt > 0) {
				int freq = Integer.parseInt(word);
				context.write(new WordWritable(new Text(currentWord),
						new IntWritable(freq)), NullWritable.get());
				break;
			} else {

				currentWord = word;
			}
			cnt++;
		}
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
