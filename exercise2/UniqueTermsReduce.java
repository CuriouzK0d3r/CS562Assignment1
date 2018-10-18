package excercise2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class UniqueTermsReduce extends Reducer<Text, Text, Text, Text> {
	private static int count = 0;
	
	@Override
	public void reduce(Text word, Iterable<Text> docs, Context context) throws IOException, InterruptedException {
		ArrayList<String> docArray = new ArrayList<String>();
		String docList = "";
		
	      for (Text doc: docs) {
	    	  String document = doc.toString();
	    	  if (!docArray.contains(document)) {
	    		  docArray.add(document);
	    		  docList += " " + document;
	    	  }
	      }
	      count++;
	      context.write(new Text(count + " " + word.toString()), new Text(docList));
	}
	
	@Override
	protected
	void cleanup(Context output){
	output.getCounter("RecordCounter","Reducer-no-"+output.getConfiguration().getInt("mapreduce.task.partition",
	                    0)).increment(count);       
	}
}
