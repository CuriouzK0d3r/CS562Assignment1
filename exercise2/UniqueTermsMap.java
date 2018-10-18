package excercise2;

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

public class UniqueTermsMap extends Mapper<LongWritable, Text, Text, Text> {

	// Word boundary defined as whitespace-characters-word boundary-whitespace
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private Set<String> patternsToSkip = new HashSet<String>();

	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

	        URI[] localPaths = context.getCacheFiles();
	        URI patternsURI = localPaths[0];
	        try {
	            BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
	            String pattern;
	            while ((pattern = fis.readLine()) != null) {
	              patternsToSkip.add(pattern);
	            }
	          } catch (IOException ioe) {
	            System.err.println("Caught exception while parsing the cached file '"
	                + patternsURI + "' : " + StringUtils.stringifyException(ioe));
	          }
	      
	}

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

	  String line = lineText.toString();
      line = line.toLowerCase();
			Pattern p = Pattern.compile("[^a-zA-Z0-9]");
			Pattern p = Pattern.compile("[0-9]+[a-zA-Z]*");

      //      line = line.replaceAll("[^A-Za-z0-9]", "");
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

    //   line = line.replaceAll("[^A-Za-z0-9]", "");
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty() || patternsToSkip.contains(word) || p.matcher(word).find()) {
            continue;
          }
          currentWord = new Text(word);
          context.write(currentWord, new Text(fileName));
        }
	}
}
