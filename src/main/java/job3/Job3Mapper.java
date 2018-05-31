package job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.RFC4180Parser;

import utils.AmazonFineFoodReviews;


public class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Elimino l'header del file csv
		if (key.get()==0)
			return;

		String line = value.toString();
		RFC4180Parser parser = new RFC4180Parser();
		String[] fields = parser.parseLine(line);

		if (fields.length == 10) {
			String user = fields[AmazonFineFoodReviews.UserId];
			String productId = fields[AmazonFineFoodReviews.ProductId];
			
			context.write(new Text(user), new Text(productId));
		}
	}
}


