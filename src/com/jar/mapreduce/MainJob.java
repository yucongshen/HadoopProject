package com.jar.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJob {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3");
		//执行的命令[root@namenode tmp]# hadoop jar wc.jar com.mr.wc.MainRunJob
		Configuration config =new Configuration();

		try {
			FileSystem fs =FileSystem.get(config);
			
			org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(config);
			job.setJarByClass(MainJob.class);
			
			job.setJobName("wordcount");
			
			job.setMapperClass(Mapper.class);
			job.setReducerClass(Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path("/mapreduce/input/"));
			
			Path outpath =new Path("/mapreduce/output/wordcount/");
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
