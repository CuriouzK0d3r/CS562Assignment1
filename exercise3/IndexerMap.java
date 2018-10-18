package excercise3;

// Basic Java file IO
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

// Java classes for working with sets
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

// Regular expression utility
import java.util.regex.Pattern;


// File I/O
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper parent class and the Configuration class
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

// Configurable counters
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

public class IndexerMap extends Mapper<LongWritable, Text, Text, Text> {

	// Word boundary defined as whitespace-characters-word boundary-whitespace
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s");

	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

	}

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

		String line = lineText.toString();
		Text currentWord = new Text();
		int cnt = 0;
		
		for (String word : WORD_BOUNDARY.split(line)) {
			if (word.isEmpty()) {
				continue;
			}
			if (cnt > 0) {
				context.write(currentWord, new Text(word));
				break;
			}
			else {
				currentWord = new Text(word);
			}
			cnt++;
		}
//		context.write(new Text(words[0]), new Text(words[1]));
	}
}
