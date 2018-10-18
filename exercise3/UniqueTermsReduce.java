package excercise3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import excercise3.DocumentWritable;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueTermsReduce extends Reducer<Text, DocumentWritable, Text, DocumentWritable> {
	@Override
	public void reduce(Text word, Iterable<DocumentWritable> docs, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> docListMap = new HashMap<String, Integer>();
		ArrayList<String> docListArray = new ArrayList<String>();
		
	      for (DocumentWritable doc: docs) {
	    	  String document = doc.getDocumentName();
	    	  if (!docListArray.contains(document)) {
	    		  docListArray.add(document);
	    		  docListMap.put(document, doc.getFreq());
	    	  }
	    	  else {
	    		  docListMap.put(document, docListMap.get(document) + doc.getFreq());
	    	  }
	      }
	      for (String doc : docListMap.keySet()) {
	    	  context.write(word, new DocumentWritable(doc, docListMap.get(doc)));
	      }
	      
	}
}
class DocumentWritable implements WritableComparable<DocumentWritable> {

	private String documentName;
	private int freq;

	public DocumentWritable(String documentName, int freq) {
		this.documentName = documentName;
		this.freq = freq;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.documentName + "#");
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

	@Override
	public int compareTo(DocumentWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
}