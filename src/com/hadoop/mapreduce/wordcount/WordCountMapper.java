package com.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
//默认情况下输入片段用数字表示为Longwriterable,值为一行一行的数据
//输出的类型以单词为界，为Text,输出的值为数字,
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//重写父类map方法，被循环调用，从文件的spilt中读取每行调用一次，把改行所在下标为key，该行的内容为value
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		String [] words=StringUtils.split(value.toString(), ' ');
		for(String w:words){
			context.write(new Text(w), new IntWritable(1));
		}
	}
}
