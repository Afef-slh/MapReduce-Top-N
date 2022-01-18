package tn.isima.topN;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;

	public class Main {
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "max share");
	        job.setJarByClass(Main.class);
	        job.setMapperClass(Map.class);
	        job.setReducerClass(Reduce.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        Path inputFilePath = new Path(args[0]);
	        Path outputFilePath = new Path(args[1]);
	        FileInputFormat.addInputPath(job, inputFilePath);
	        FileOutputFormat.setOutputPath(job, outputFilePath);
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	}
