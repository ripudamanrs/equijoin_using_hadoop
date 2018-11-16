
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map Reduce Program for Equijoin
 *
 * @author ripudamansingh
 */

public class equijoin {

	// Store tablenames in global variables
	public static String table1Name = "";
	public static String table2Name = "";

	/**
	 * Mapper creates a key:column to be joined and value:line
	 * 
	 * @author ripudamansingh
	 */
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text joinKey = new Text();
		private Text joinValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			String[] tokens = value.toString().split(",");
			if (valueString != null) {
				if (table1Name.isEmpty()) {
					table1Name = tokens[0].trim();
				} else {
					if (!table1Name.equals(tokens[0].trim())) {
						table2Name = tokens[0].trim();
					}
				}
				joinKey.set(tokens[1]);
				joinValue.set(valueString);
				context.write(joinKey, joinValue);
			}
		}
	}

	/**
	 * Reducer: inserts tuples to maps of (table+column) keys and then joins for
	 * all common (column) keys.
	 * @author ripudamansingh
	 */
	public static class EquijoinReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, List<String>> memoMap1 = new HashMap<>();
			Map<String, List<String>> memoMap2 = new HashMap<>();
			String strData = "";
			// for each value(line) from mapper
			for (Text val : values) {
				String valueString = val.toString();
				String[] tokens = val.toString().split(",");
				// if current tuple is from table1
				if (tokens != null && tokens[0].trim().equals(table1Name)) {
					// check if the same column tuple exists for table2 in map2
					if (memoMap2.get(table2Name + tokens[1]) != null) {
						// if it does, join all entries with this and append to output
						for (String s : memoMap2.get(table2Name + tokens[1])) {
							strData = valueString + ", " + s;
							result.set(strData);
							context.write(result, NullWritable.get());
						}
					} else {
						// if it doesnt, add to the other map or create a new list if the other map doesnt have the entry
						if (memoMap1.get(table1Name + tokens[1]) != null) {
							List<String> value = memoMap1.get(table1Name + tokens[1]);
							value.add(valueString);
							memoMap1.put(table1Name + tokens[1], value);
						} else {
							List<String> value = new ArrayList<>();
							value.add(valueString);
							memoMap1.put(table1Name + tokens[1], value);
						}
					}
				} else if (tokens != null && tokens[0].trim().equals(table2Name)) {
					// else, if current tuple is from table2
					// check if the same column tuple exists for table1 in map1
					if (memoMap1.get(table1Name + tokens[1]) != null) {
						// if it does, join all entries with this and append to output
						for (String s : memoMap1.get(table1Name + tokens[1])) {
							strData = valueString + ", " + s;
							result.set(strData);
							context.write(result, NullWritable.get());
						}
					} else {
						// if it doesnt, add to the other map or create a new list if the other map doesnt have the entry
						if (memoMap2.get(table2Name + tokens[1]) != null) {
							List<String> value = memoMap2.get(table2Name + tokens[1]);
							value.add(valueString);
							memoMap2.put(table2Name + tokens[1], value);
						} else {
							List<String> value = new ArrayList<>();
							value.add(valueString);
							memoMap2.put(table2Name + tokens[1], value);
						}
					}
				}
			}
		}	
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "equijoin");
		job.setJarByClass(equijoin.class);
		// defining mapper and reducer classes
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(EquijoinReducer.class);
		// defining type of mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// defining type of reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
