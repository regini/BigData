package job1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.RFC4180Parser;

import utils.AmazonFineFoodReviews;
import utils.UnixTimeConverter;

public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Elimino l'header del file csv
		if (key.get()==0)
			return;

		String line = value.toString();
		RFC4180Parser parser = new RFC4180Parser();
		String[] fields = parser.parseLine(line);

		if (fields.length == 10) {
			IntWritable year = UnixTimeConverter.time2Year(fields[AmazonFineFoodReviews.Time]);
			StringTokenizer tokenizer = new StringTokenizer(fields[AmazonFineFoodReviews.Summary].toLowerCase());
			Text word = new Text();
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(year, word);
			}
		}
	}
}


