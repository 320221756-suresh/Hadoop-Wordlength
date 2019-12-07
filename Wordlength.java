import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Wordlength {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private IntWritable word = new IntWritable();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().length());
            context.write(word, one);
        }
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

	
	    int t, s, m, b;
	    Text tiny = new Text("tiny");
	    Text small = new Text("small");
	    Text medium = new Text("medium");
	    Text big = new Text("big");
	    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{        
	        for (IntWritable val:values){
	            if(key.get() == 1){
	                t += val.get();                         
	                }
	            else if(2<=key.get() && key.get()<=4){
	                s += val.get();             
	                }
	            else if(5<=key.get() && key.get()<=9){
	                m += val.get();             
	                }
	            else if(10<=key.get()){
	                b += val.get();             
	                }

	        }
	       
	        context.write(tiny, new IntWritable(t));
	        context.write(small,new IntWritable(s));
	        context.write(medium, new IntWritable(m));
	        context.write(big,new IntWritable(b)); 
	    }
	    }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordlength");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

