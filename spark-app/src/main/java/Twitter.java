
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class Twitter {


	public static void main(String[] args) throws InterruptedException {
	
		
		

		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			Logger.getRootLogger().setLevel(Level.WARN);
		}
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		//String[] filters = { "Warsaw" };

		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		String master = "local[2]";

		SparkConf sparkConf = new SparkConf().setAppName("Twitter").setMaster(master);

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);

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

		JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				// leave out the # character
				return new Tuple2<String, Integer>(s.substring(1), 1);
			}
		});

		/*
		 * Read in the word-sentiment (ref
		 * https://github.com/apache/bahir/blob/master/streaming-twitter/
		 * examples/data/AFINN-111.txt list and create a static RDD from it
		 */
		String wordsValue = "/home/ord4k/Documents/stream.txt";
		final JavaPairRDD<String, Double> wordSentiments = ssc.sparkContext().textFile(wordsValue)
				.mapToPair(new PairFunction<String, String, Double>() {
					public Tuple2<String, Double> call(String line) {
						String[] columns = line.split("\t");
						return new Tuple2<String, Double>(columns[0], Double.parseDouble(columns[1]));
					}
				});
		
		// hashTags.dstream().saveAsTextFiles("file:///home/ord4k/Documents/sparkResult/stream2.txt","txt");
		hashTags.print();
		
		//This needs to be checked
		/* JavaDStream<Long> requestCountRBW = hashTags.map(new Function<String, Long>() {
		        public Long call(String entry) {
		          return 1L;
		        }}).reduceByWindow(new Function2<Long, Long, Long>() {
		            public Long call(Long v1, Long v2) {
		              return v1+v2;
		            }}, new Function2<Long, Long, Long>() {
		            public Long call(Long v1, Long v2) {
		              return v1-v2;
		            }}, Durations.seconds(120), Durations.seconds(120));
		requestCountRBW.print();*/
		
		
		ssc.start();

		ssc.awaitTermination();

	}

}
