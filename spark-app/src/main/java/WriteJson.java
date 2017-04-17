import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Iterable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

public class WriteJson implements FlatMapFunction<Iterator<Person>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5555402033451929182L;

	public Iterable<String> call(Iterator<Person> people) throws Exception {
		ArrayList<String> text = new ArrayList<String>();
		ObjectMapper mapper = new ObjectMapper();
		while (people.hasNext()) {
			Person person = people.next();
			text.add(mapper.writeValueAsString(person));
		}
		return text;
	}

	public static void main(String[] args) {

		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local[10]").setAppName("parseJson");
		// String inputFile = "/usr/lib/spark/README.md";

		// Create a Java Spark Context.

		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile("/home/ord4k/Documents/json.txt");
		JavaRDD<Person> result = input.mapPartitions(new ParseJson());

		/*
		 * Use some filter.... JavaRDD<Person> result = input.mapPartitions(new
		 * ParseJson()).filter( new People());
		 */

		JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
		formatted.saveAsTextFile("/home/ord4k/Documents/resultJson");
		sc.close();
	}
}