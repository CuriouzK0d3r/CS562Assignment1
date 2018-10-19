package exercise3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DocumentWritable implements Writable {

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
}
