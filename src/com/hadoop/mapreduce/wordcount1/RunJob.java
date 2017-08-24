package com.hadoop.mapreduce.wordcount1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Iterator;

public class RunJob {

	public static void main(String[] args) {
//		System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3");
		//执行的命令[root@namenode tmp]# hadoop jar wc.jar com.mr.wc.MainRunJob
		Configuration config =new Configuration();
		Iterator iterator=config.iterator();

		while(iterator.hasNext()){
			System.out.println(iterator.next().toString());
		}
		try {
			FileSystem fs =FileSystem.get(config);
			
			Job job =Job.getInstance(config);
			job.setJarByClass(RunJob.class);
			
			job.setJobName("wc");
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			
			Path outpath =new Path(args[1]);
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean f= job.waitForCompletion(true);
			if(f){
				System.out.println("job任务执行成功");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
