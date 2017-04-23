import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class Twitter {

	public static void main(String[] args) {

		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			Logger.getRootLogger().setLevel(Level.WARN);
		}
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		String[] filters = Arrays.copyOfRange(args, 4, args.length);

		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		String master = "local[2]";

		SparkConf sparkConf = new SparkConf().setAppName("Twitter").setMaster(master);

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, filters);

		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {

			public Iterable<String> call(Status s) {
				return Arrays.asList(s.getText().split(" "));
			}
		});

		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			public Boolean call(String word) {
				return word.startsWith("#");
			}
		});


		// Read in the word-sentiment list and create a static RDD from it
		String twitterStreamResult = "/home/ord4k/Documents/stream.txt";
		final JavaPairRDD<String, Double> wordSentiments = ssc.sparkContext().textFile(twitterStreamResult)
				.mapToPair(new PairFunction<String, String, Double>() {
					public Tuple2<String, Double> call(String line) {
						String[] columns = line.split("\t");
						return new Tuple2<String, Double>(columns[0], Double.parseDouble(columns[1]));
					}
				});
		System.out.println(wordSentiments.collect());

	}

}
