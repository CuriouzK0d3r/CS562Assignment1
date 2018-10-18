package excercise3;

// Basic Java file IO
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
// Regular expression utility
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import excercise3.DocumentWritable;

// Java classes for working with sets
// File I/O
// Mapper parent class and the Configuration class
// Wrappers for values
// Configurable counters

public class UniqueTermsMap extends Mapper<LongWritable, Text, Text, DocumentWritable> {

	// Word boundary defined as whitespace-characters-word boundary-whitespace
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private Set<String> patternsToSkip = new HashSet<String>();

	protected void setup(Mapper<LongWritable, Text, Text, DocumentWritable>.Context context)
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

      //      line = line.replaceAll("[^A-Za-z0-9]", "");
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

    //   line = line.replaceAll("[^A-Za-z0-9]", "");
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty() || patternsToSkip.contains(word) || p.matcher(word).find()) {
            continue;
          }
          currentWord = new Text(word);
          context.write(currentWord, new DocumentWritable(fileName, 1));
        }
	}
}

class DocumentWritable implements Writable , Serializable {

	private String documentName;
	private int freq;

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
		return this.documentName + this.freq;
	}
}