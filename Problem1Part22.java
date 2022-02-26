import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Problem1Part22 {

	public static class CountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private final static DoubleWritable one = new DoubleWritable(1.0);
		private final static DoubleWritable zer = new DoubleWritable(0.0);
		private Text word = new Text("Noun");
		
		private POSModel model = new POSModelLoader().load(new File("/home/sahaj/files/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin")); //Edit path to the pre-trained model file
		private POSTaggerME tagger = new POSTaggerME(model);
		
		private SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokenizedLine[] = tokenizer.tokenize(line);
	    	String[] tags = tagger.tag(tokenizedLine);
			for (String token : tags) {
				if(token.equals("NOUN")) context.write(word, one);
				else context.write(word, zer);
			}
		}
	}

	public static class CountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private Text word = new Text("Noun Percentage");

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double nc = 0.0,tc=0.0;
			for (DoubleWritable val : values) {
				nc += val.get();
				tc+=1.0;
			}
			result.set((double)((nc*1.0)/(tc*1.0)));
			context.write(word, result);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "allcount");

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}