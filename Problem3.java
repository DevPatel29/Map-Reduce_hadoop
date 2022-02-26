import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Problem3 {

	public static class WordMapper extends Mapper<Object, Text, Text, Text> {
		private Text key_text = new Text();
		private Text val_text = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+");
				
			key_text.set(tokens[0]);
			val_text.set(tokens[1]);
			context.write(key_text, val_text);
		}
	}
	
	public static class UrlMapper extends Mapper<Object, Text, Text, Text> {
		private Text key_text = new Text();
		private Text val_text = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\s+");
				
			key_text.set(tokens[0]);
			
			String temp = "URL: " + tokens[1];
			val_text.set(temp);
			
			context.write(key_text, val_text);
		}
	}

	public static class Query1_Reducer extends Reducer<Text, Text, Text, BooleanWritable> {
		private Text key_text = new Text();
		private BooleanWritable result = new BooleanWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String URL = "temporary_URL";
			Set<String> hash_set = new HashSet<String>();
			
			for(Text t:values) {
				String temp = t.toString();
				
				if(temp.equals("infantri") || temp.equals("reinforc") || temp.equals("brigad") || temp.equals("fire")) {
					hash_set.add(temp);
				}
				
				if(temp.length() >= 4 && temp.substring(0,4).equals("URL:")) {
					URL = temp.substring(5);
				}
			}
			
			result.set(false);
			
			if(hash_set.size() == 4) {
				result.set(true);
			}
			
			key_text.set(URL);
			context.write(key_text, result);
		}
	}

	public static class Query2_Reducer extends Reducer<Text, Text, Text, BooleanWritable> {
		private Text key_text = new Text();
		private BooleanWritable result = new BooleanWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String URL = "temp";
			Set<String> hash_set = new HashSet<String>();
			
			for(Text t:values) {
				String temp = t.toString();
				
				if(temp.equals("infantri") || temp.equals("reinforc") || temp.equals("brigad") || temp.equals("fire")) {
					hash_set.add(temp);
				}
				
				if(temp.length() >= 4 && temp.substring(0,4).equals("URL:")) {
					URL = temp.substring(5);
				}
			}
			
			result.set(false);
			
			if(hash_set.size() >= 1) {
				result.set(true);
			}
			
			key_text.set(URL);
			context.write(key_text, result);
		}
	}
	
	public static class Query3_Reducer extends Reducer<Text, Text, Text, BooleanWritable> {
		private Text key_text = new Text();
		private BooleanWritable result = new BooleanWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String URL = "temp";
			Set<String> hash_set = new HashSet<String>();
			
			for(Text t:values) {
				String temp = t.toString();
				
				if(temp.equals("infantri") || temp.equals("reinforc") || temp.equals("brigad") || temp.equals("fire")) {
					hash_set.add(temp);
				}
				
				if(temp.length() >= 4 && temp.substring(0,4).equals("URL:")) {
					URL = temp.substring(5);
				}
			}
			
			result.set(false);
			
			if(hash_set.size() == 1 && hash_set.contains("reinforc")) {
				result.set(true);
			}
			
			key_text.set(URL);
			context.write(key_text, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Scanner sc= new Scanner(System.in); 

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "problem3");
		job.setJarByClass(Problem3.class);
		

		System.out.println("Please Enter the Query you want to execute: \n1 -- Query 1  \n2 -- Query 2  \n3 -- Query 3");
		int input = sc.nextInt();
		
		if(input == 1) {
			job.setReducerClass(Query1_Reducer.class);
		}
		else if(input == 2) {
			job.setReducerClass(Query2_Reducer.class);
		}
		else if(input == 3) {
			job.setReducerClass(Query3_Reducer.class);
		}
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, WordMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, UrlMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		sc.close();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}