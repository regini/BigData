package job2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.RFC4180Parser;

import utils.AmazonFineFoodReviews;
import utils.ScoreByYear;
import utils.UnixTimeConverter;


public class Job2Mapper extends Mapper<LongWritable, Text, Text, ScoreByYear> {

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
			IntWritable score = new IntWritable(Integer.parseInt(fields[AmazonFineFoodReviews.Score]));
		
			ScoreByYear s = new ScoreByYear(score, year);

			context.write(new Text(fields[AmazonFineFoodReviews.ProductId]), s);
		}
	}
}


