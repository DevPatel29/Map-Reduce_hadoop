import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import opennlp.tools.stemmer.PorterStemmer;

public class Problem2Part1 {

	public static class DFMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text fname = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			fname.set(filename);
			
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+");
			PorterStemmer steammer = new PorterStemmer();

			for (String token : tokens) {
				word.set(steammer.stem(token).toString());
				context.write(word, fname);
			}
		}
	}

	public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> fnameset = new HashSet<String>();
			for (Text val : values) {
				fnameset.add(val.toString());
			}
			result.set(fnameset.size());
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DF");

		job.setMapperClass(DFMapper.class);
		job.setReducerClass(DFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}