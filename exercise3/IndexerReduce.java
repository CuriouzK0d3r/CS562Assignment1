package excercise3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class IndexerReduce extends Reducer<Text, Text, Text, Text> {
	private static int count = 0;

	private static final Logger LOGGER = LogManager.getRootLogger();

	@Override
	public void reduce(Text word, Iterable<Text> docs, Context context)
			throws IOException, InterruptedException {
		long cn = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
		LOGGER.info("gamiese");
		String docList = "";
				
		for (Text docText : docs) {
			String doc = docText.toString();
			doc = doc.replaceAll("###", "#");
			docList += doc + ' ';
		}
		++count;
		word = new Text(count + " " + word.toString());
		context.write(word, new Text(docList));
	}
	
	@Override
	protected
	void cleanup(Context output){
	output.getCounter("RecordCounter","Reducer-no-"+output.getConfiguration().getInt("mapreduce.task.partition",
	                    0)).increment(count);       
	}
}
