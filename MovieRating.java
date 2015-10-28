import java.io.IOException;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRating {
	
	
	public static class MapAvg extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static Text output = new Text();
		private static IntWritable output_value = new IntWritable();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	  
		
			StringTokenizer token = new StringTokenizer(value.toString());
			String abc[]=value.toString().split("::");
			output.set(abc[1]);
			output_value.set(Integer.parseInt(abc[2]));
			context.write(output,output_value);
			
		}
	}
	    
	// this is reducer of Job-1 : you can always write it in a different java class file
	public static class ReduceAvg extends Reducer<Text, IntWritable, Text, Text> {
		private Text avg_out = new Text();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			
			int sum = 0; // initialize the sum for each keyword
			int temp = 0;
			for (IntWritable val : values) {
				sum += val.get();
				temp++;
			}
			float avg=(float)sum/temp;
			avg_out.set(" "+String.valueOf(avg));
			context.write(key, avg_out);
		}
	}
	
	
	public static class Map2 extends Mapper<Object, Text, NullWritable, Text> {
		private PriorityQueue<TopComparator> priority = new PriorityQueue<TopComparator>();
      	
             
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		      String[] v = value.toString().split(" ");
	                
	               TopComparator c = new TopComparator(v[0],Double.parseDouble(v[1])) ;
	               priority.add(c);
		    }
	              
	              @Override
	              protected void cleanup(Context context) throws IOException,
	                InterruptedException {
	                // Output our ten records to the reducers with a null key
	                int k=0;
	                  while (k<10)
	                {
	                   TopComparator c=priority.poll();
	                   context.write(NullWritable.get(), new Text(c.movieId+","+c.averageRating));
	                 k++;  
	                }
	                }
           }
      
 
	public static class Reduce2 extends Reducer<NullWritable, Text, Text, Text> {
	    private PriorityQueue<TopComparator> pr = new PriorityQueue<TopComparator>();
	    public void reduce(Text key, Iterable <Text> values, Context context) 
	      throws IOException, InterruptedException {
	        
	        for (Text val : values) {
	            String[] v = val.toString().split(",");
	                
	            TopComparator c = new TopComparator(v[0],Double.parseDouble(v[1])) ;
	            pr.add(c);
	            
	       }
	         int k=0;
	                  while (k<10 && !pr.isEmpty())
	                {
	                   TopComparator c=pr.poll();
	                   context.write(new Text(c.movieId),new Text(Double.toString(c.averageRating)));
	                 k++;  
	                }
	       
	    }
	 }
	    
	  static class TopComparator implements Comparable<TopComparator> 
	{ 
		private String movieId;
		private double averageRating;
		public TopComparator(String movieId, double average_rating) 
		{ 
		 
		this.movieId = movieId; 
		this.averageRating = average_rating; 
		} 
		@Override 
		public int compareTo(TopComparator o)
		{
		if(averageRating < o.averageRating)
		{
		return 1;
		}
		else
		{
		return -1;
		}
		}	
	}
	
	
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, IllegalStateException, InterruptedException
	{
		Configuration conf = new Configuration();  
		   
		if (args.length != 3) {
			  System.err.println("Incorrect number of arguments");
			  System.exit(2);
			}
		
	    Job job = new Job(conf,"rating");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(MovieRating.class);
	    job.setMapperClass(MapAvg.class); 
	    job.setReducerClass(ReduceAvg.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    if (job.waitForCompletion(true))
        {
            Job job1 = new Job(conf, "Top10order");
            job1.setJarByClass(MovieRating.class);
            job1.setMapperClass(Map2.class);
           
            job1.setReducerClass(Reduce2.class);

            job1.setOutputKeyClass(NullWritable.class);
            job1.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job1, new Path(args[1]+"/part-r-00000"));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            
            System.exit(job1.waitForCompletion(true) ? 0 : 1);
      }
	}

}
