import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;

public class Problem1Part1 {

	public static class CountMapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text wordn = new Text("Noun");
		
		private POSModel model = new POSModelLoader().load(new File("/home/sahaj/files/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin")); //Edit path to the pre-trained model file
		private POSTaggerME tagger = new POSTaggerME(model);
		
		private SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokenizedLine[] = tokenizer.tokenize(line);
	    	String[] tags = tagger.tag(tokenizedLine);
			for (String token : tags) {
				if(token.equals("NOUN")) context.write(wordn, one);
			}
		}
	}

	public static class CountReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}
	
	public static class CountMapper2 extends Mapper<Object, Text, Text, IntWritable> {

		private IntWritable cnt = new IntWritable();
		private Text wordt = new Text("Total");
		
		private SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokenizedLine[] = tokenizer.tokenize(line);
			cnt.set(tokenizedLine.length);
			context.write(wordt, cnt);
		}
	}

	public static class CountReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "nouncount");

		job1.setMapperClass(CountMapper1.class);
		job1.setReducerClass(CountReducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);
		
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "totalcount");

		job2.setMapperClass(CountMapper2.class);
		job2.setReducerClass(CountReducer2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}