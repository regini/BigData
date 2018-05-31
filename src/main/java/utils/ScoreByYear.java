package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ScoreByYear implements Writable{

	private IntWritable score;
	private IntWritable year;

	public ScoreByYear() {
		this.score=new IntWritable();
		this.year=new IntWritable();
	}
	
	public ScoreByYear(IntWritable score, IntWritable year) {
		this.score = score;
		this.year = year;
	}

	public void readFields(DataInput in) throws IOException {
		score = new IntWritable(Integer.valueOf(in.readUTF()));
		year = new IntWritable(Integer.valueOf(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(score.toString());
		out.writeUTF(year.toString());
	}

	public int getScore() {
		return score.get();
	}

	public void setScore(int score) {
		this.score = new IntWritable(score);
	}

	public int getYear() {
		return year.get();
	}
	
	public void setYear(int year) {
		this.year = new IntWritable(year);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((score == null) ? 0 : score.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScoreByYear other = (ScoreByYear) obj;
		if (score == null) {
			if (other.score != null)
				return false;
		} else if (!score.equals(other.score))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}
	
	 @Override
	    public String toString() {
	        return score.toString() + " " + year.toString();
	    }
}