
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ZipCode {
	
	private static String zipcode1;
	

	public static class Map extends Mapper<LongWritable, Text,IntWritable, NullWritable>{
		private final static IntWritable output = new IntWritable();
		private final static NullWritable outputvalue =NullWritable.get();
		
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(value.toString());
			String value[]=value.toString().split("::");
			// line to string token
			
			
			
			
			if(zipcode1.equalsIgnoreCase(value[4]))
			{
			output.set(Integer.parseInt(value[0]));
				// set word as each input keyword
				context.write(output, outputvalue);  // create a pair <keyword, 1> 
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable>
	{
		private NullWritable result = NullWritable.get();
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
			
			//result.set(sum);
			context.write(key, result); // create a pair <keyword, number of occurences>
		}
	}
	
// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		
				
		zipcode1=args[0];
		
		Job job = new Job(conf, "zipid");
		job.setJarByClass(ZipCode.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

   //job.setCombinerClass(Reduce.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
