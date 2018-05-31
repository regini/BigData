package job3;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import com.opencsv.RFC4180Parser;

import scala.Tuple2;
import utils.AmazonFineFoodReviews;
import utils.TupleComparator;


public class Job3Spark implements Serializable{

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;

	public Job3Spark(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {
		double startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Job3Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);;

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job3Spark bpm = new Job3Spark(args[0], args[1]);
		bpm.run(sc);

		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		sc.close();
		sc.stop();
		System.out.println("Tempo di esecuzione Job3 Spark:\t" + elapsedTime + "s");
	}

	private void run(JavaSparkContext sc) {
		JavaPairRDD<String, String> data = loadData(sc);
		JavaPairRDD<String, Tuple2<String, String>> joinedData = data.join(data);
		JavaPairRDD<Tuple2<String, String>, String> prod2user = joinedData.mapToPair(x -> {
			return new Tuple2<Tuple2<String, String>, String>(x._2, x._1);
		});
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> list = prod2user.groupByKey();
		JavaPairRDD<Tuple2<String, String>, Integer> result = list.mapToPair(x -> { 
			HashSet<String> users = new HashSet<>();
			x._2.forEach(y -> {
				users.add(y);
			});
			Integer i = new Integer(users.size());
			return new Tuple2<>(x._1, i);
		});
		result.sortByKey(new TupleComparator(), true).saveAsTextFile(outputFolderPath);
	}

	public JavaPairRDD<String, String> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputFilePath);

		JavaPairRDD<String, String> data = 
				words.mapPartitionsWithIndex((index, iter) -> {
					if (index == 0 && iter.hasNext()) {
						iter.next();
						if (iter.hasNext()) {
							iter.next();
						}
					}
					return iter;
				}, true)
				.mapToPair(row -> {
					RFC4180Parser parser = new RFC4180Parser();
					String[] values = parser.parseLine(row);

					return new Tuple2<String, String>(values[AmazonFineFoodReviews.UserId],
							values[AmazonFineFoodReviews.ProductId]
							);
				});
		return data;
	}
}