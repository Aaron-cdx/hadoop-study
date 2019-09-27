package com.duanxi.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(FirstJob.class);
			job.setJobName("weibo1");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// 这里设置了四个reduce任务，也就是在数据中会产生四个分区
			// 此时可以按照我们设定的分区来对分区的数据做一个功能划分
			job.setNumReduceTasks(4);
			job.setPartitionerClass(FirstPartition.class);
			job.setMapperClass(FirstMapper.class);
			job.setCombinerClass(FirstReduce.class);
			job.setReducerClass(FirstReduce.class);

			FileInputFormat.addInputPath(job, new Path("/data/tfidf/input/"));

			Path path = new Path("/data/tfidf/output/weibo1");
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			FileOutputFormat.setOutputPath(job, path);

			boolean f = job.waitForCompletion(true);
			if (f) {

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
