package com.hadoop.mapreduce.usercount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shen_ on 2017/8/23.
 */
public class ArticleUsersCount extends Configured implements Tool {
    /**
     * Mapper类
     * @author shenyucong
     */
    public static class TestMapper extends Mapper<LongWritable, Text, Entity, Text> {
        private final Entity outputKey = new Entity();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] allData = value.toString().split("\t");
            int userLength = allData.length-1;
            int userNum = userLength;
            outputKey.setMyKey(userNum);
            String valueString = value.toString().replace("\t",",");
            outputValue.set(valueString);
            context.write(outputKey, outputValue);
        }
    }
    public static class TestReducer extends Reducer<Entity, Text, IntWritable, Text> {
        private final IntWritable outputKey = new IntWritable();
        private final Text outputValue = new Text();
        static  Counter counter;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            counter=new GenericCounter("reduce","reduceCounter");
            super.setup(context);
        }

        @Override
        protected void reduce(Entity key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int countValue = Integer.parseInt(conf.get("counterValue"));
            outputKey.set(key.getMyKey());
            if(counter.getValue() < countValue) {
                for (Text v : values) {
                    outputValue.set(v);
                    context.write(outputKey, outputValue);
                    counter.increment(1L);
                }
            }
        }
    }
    public static class Entity implements WritableComparable<Entity>{
        private int myKey;

        public int getMyKey() {
            return myKey;
        }

        public void setMyKey(int myKey) {
            this.myKey = myKey;
        }

        @Override
        public int compareTo(Entity o) {
            return o.getMyKey()-this.getMyKey();
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            myKey = dataInput.readInt();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(myKey);
        }
    }
    /**
     * 用于map端和reduce端排序的比较器:articleIdAndUsers相同，则比较用户数量
     * @author shenyucong
     */
    public static class TestComparator extends WritableComparator {

        public TestComparator() {
            super(Entity.class,true);
        }

        @Override
        @SuppressWarnings("all")
        public int compare(Object a, Object b) {
            Entity o1 = (Entity) a;
            Entity o2 = (Entity) b;
            return o1.getMyKey() - o2.getMyKey();
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3");
        Configuration conf = getConf();
        //设置reduce的条数（reduce方法中获取计数器的值）
        conf.set("counterValue","100");
        Job job = Job.getInstance(conf, ArticleUsersCount.class.getSimpleName());
        job.setJarByClass(ArticleUsersCount.class);

        FileSystem fs =FileSystem.get(conf);
        FileInputFormat.addInputPath(job, new Path("D:\\IdeaProjects\\HadoopDemo\\input"));
        Path outpath =new Path("D:\\IdeaProjects\\HadoopDemo\\output\\result");
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        // map settings
        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Entity.class);
        job.setMapOutputValueClass(Text.class);

        // reduce settings
        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);

        job.setSortComparatorClass(TestComparator.class);

        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ArticleUsersCount(), args);
    }
}
