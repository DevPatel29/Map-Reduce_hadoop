import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import opennlp.tools.stemmer.PorterStemmer;

public class Problem2Part2 {
	
	private static HashMap<String, Integer> dfreq = new HashMap<>();

	public static class TFMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text fname = new Text();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			URI dfUri = Job.getInstance(conf).getCacheFiles()[0];
			Path dfPath = new Path(dfUri.getPath());
			String dfFileName = dfPath.getName().toString();
			BufferedReader reader = new BufferedReader(new FileReader(dfFileName));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("[^\\w']+");
				if(tokens.length==2) {
					dfreq.put(tokens[0], Integer.parseInt(tokens[1]));
				}
			}
			reader.close();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			fname.set(filename.toString());
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+");
			PorterStemmer steammer = new PorterStemmer();

			for (String token : tokens) {
				String s=steammer.stem(token).toString();
				word.set(s+"\t"+filename);
				context.write(word, one);
			}
		}
	}

	public static class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private Text word = new Text();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String[] keytokens = key.toString().split("\t");
			int df = dfreq.get(keytokens[0]);
			
			int tf=0;
			for (IntWritable val : values) {
				tf+=val.get();
			}
			
			double score = tf * (double)Math.log(1/df+1);
			String st = String.format("%s\t%s", keytokens[1],keytokens[0]);
			
			word.set(st);
			result.set(score);
			context.write(word, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DF");

		job.setMapperClass(TFMapper.class);
		job.setReducerClass(TFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.addCacheFile(new Path(args[0]).toUri());

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}