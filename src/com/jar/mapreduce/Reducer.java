package com.jar.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
	//每组调用一次，这一组数据特点：key相同，value可能有多个
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable i:arg1){
			sum+=i.get();
		}
		context.write(arg0, new IntWritable(sum));
	}
}
