package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TermFreq implements WritableComparable<TermFreq> {

	private Text term;
	private IntWritable frequency;

	public TermFreq() {
	}
	
	public TermFreq(String term) {
		this.term = new Text(term);
		this.frequency = new IntWritable(1);
	}
	public TermFreq(Text term) {
		this.term = term;
		this.frequency = new IntWritable(1);
	}
	
	public TermFreq(Text term, IntWritable frequency) {
		this.term = term;
		this.frequency = frequency;
	}

	public void incrementFreq() {
		this.frequency = new IntWritable(this.frequency.get() + 1);
	}
	
	public void readFields(DataInput in) throws IOException {
		term = new Text(in.readUTF());
		frequency = new IntWritable(Integer.valueOf(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(term.toString());
		out.writeUTF(frequency.toString());
	}

	public void set(Text term, IntWritable frequency) {
		this.term = term;
		this.frequency = frequency;
	}

	public Text getTerm() {
		return term;
	}

	public void setTerm(Text term) {
		this.term = term;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}

	@Override
	public String toString() {
		return term.toString() + " " + frequency.toString();
	}

	@Override
	public int hashCode() {
		return term.hashCode() + frequency.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TermFreq) {
			TermFreq termFreq = (TermFreq) o;
			return term.equals(termFreq.term)
					&& frequency.equals(termFreq.frequency);
		}
		return false;
	}

	public int compareTo(TermFreq tf) {
		return tf.frequency.compareTo(this.frequency);
	}
}