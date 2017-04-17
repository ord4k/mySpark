
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Iterable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;


public class ParseJson implements FlatMapFunction<Iterator<String>, Person> {

	private static final long serialVersionUID = 1L;
	// example 5.8

	public Iterable<Person> call(Iterator<String> lines) throws Exception {
		ArrayList<Person> people = new ArrayList<Person>();
		ObjectMapper mapper = new ObjectMapper();
		while (lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line, Person.class));
			} catch (Exception e) {
				// skip records on failure
			}
		}
		
		return people;
		
	}

	
	public static void main(String[] args) {

		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local[10]").setAppName("parseJson");
		//String inputFile = "/usr/lib/spark/README.md";

		// Create a Java Spark Context.

		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile("/home/ord4k/Documents/json.txt");
		JavaRDD<Person> result = input.mapPartitions(new ParseJson());
		//System.out.println(result.collect());

		
		sc.close();

	}

}
