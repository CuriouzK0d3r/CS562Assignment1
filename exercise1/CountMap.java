package exercise1;

// Basic Java file IO
import java.io.IOException;

// Regular expression utility
import java.util.regex.Pattern;


// Mapper parent class and the Configuration class
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	// IntWritable object set to the value 1 as counting increment.
	private final static IntWritable one = new IntWritable(1);
	// Word boundary defined as whitespace-characters-word boundary-whitespace
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)	throws IOException, InterruptedException {}

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

		String line = lineText.toString();
		line = line.toLowerCase();
		Pattern p = Pattern.compile("[^a-zA-Z0-9]");
		line = line.replaceAll("[^A-Za-z0-9]", " ");

		Text currentWord = new Text();
		for (String word : WORD_BOUNDARY.split(line)) {
			if (!word.isEmpty() && !p.matcher(word).find()) {
				currentWord = new Text(word);
				context.write(currentWord, one);
			}
		}

	}
}
