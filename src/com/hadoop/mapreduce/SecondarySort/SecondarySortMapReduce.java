package com.hadoop.mapreduce.SecondarySort;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortMapReduce extends Configured implements Tool {

    /**
     * 消费信息
     * @author Ivan
     *
     */
    public static class CostBean implements WritableComparable<CostBean> {
        private String account;
        private double cost;

        public void set(String account, double cost) {
            this.account = account;
            this.cost = cost;
        }

        public String getAccount() {
            return account;
        }

        public double getCost() {
            return cost;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            byte[] buffer = account.getBytes(Charset.forName("UTF-8"));
            out.writeInt(buffer.length);                // 账户的字节流长度. out.writeUTF()使用的编码方式很复杂，需要使用DataInput.readUTF()来解码，这里不这么用
            out.write(buffer);
            out.writeDouble(cost);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int accountLength = in.readInt();
            byte[] bytes = new byte[accountLength];
            in.readFully(bytes);
            account = new String(bytes);
            cost = in.readDouble();
        }

        @Override
        public int compareTo(CostBean o) {
            if (account.equals(o.account)) {        //账户相等, 接下来比较消费金额 
                return cost == o.cost ? 0 : (cost > o.cost ? 1 : -1);
            }
            return account.compareTo(o.account);
        }

        @Override
        public String toString() {
            return account + "\t" + cost;
        }
    }

    /**
     * 用于map端和reduce端排序的比较器:如果账户相同，则比较金额
     * @author Ivan
     *
     */
    public static class CostBeanComparator extends WritableComparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int accountLength1 = readInt(b1, s1);
            int accountLength2 = readInt(b2, s2);

            int result = compareBytes(b1, s1 + 4, accountLength1, b2, s2 + 4, accountLength2);
            if (result == 0) {  // 账户相同，则比较金额 
                double thisValue = readDouble(b1, s1 + 4 + accountLength1);
                double thatValue = readDouble(b2, s2 + 4 + accountLength2);
                return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
            } else {
                return result;
            }
        }
    }

    /**
     * 用于map端在写磁盘使用的分区器
     * @author Ivan
     *
     */
    public static class CostBeanPatitioner extends Partitioner<CostBean, DoubleWritable> {

        /**
         * 根据 account分区
         */
        @Override
        public int getPartition(CostBean key, DoubleWritable value, int numPartitions) {
            return key.account.hashCode() % numPartitions;
        }
    }

    /**
     * 用于在reduce端分组的比较器根据account字段分组,即相同account的作为一组
     * @author Ivan
     *
     */
    public static class GroupComparator extends WritableComparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int accountLength1 = readInt(b1, s1);
            int accountLength2 = readInt(b2, s2);

            byte[] tmpb1 = new byte[accountLength1];
            byte[] tmpb2 = new byte[accountLength2];
            System.arraycopy(b1, s1 + 4, tmpb1, 0, accountLength1);
            System.arraycopy(b2, s2 + 4, tmpb2, 0, accountLength2);

            String account1 = new String(tmpb1, Charset.forName("UTF-8"));
            String account2 = new String(tmpb1, Charset.forName("UTF-8"));

            System.out.println("grouping: accout1=" + account1 + ", accout2=" + account2);

            return compareBytes(b1, s1 + 4, accountLength1, b2, s2 + 4, accountLength2);
        }
    }

    /**
     * Mapper类
     * @author Ivan
     *
     */
    public static class SecondarySortMapper extends Mapper<LongWritable, Text, CostBean, DoubleWritable> {
        private final CostBean outputKey = new CostBean();
        private final DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            double cost = Double.parseDouble(data[1]);
            outputKey.set(data[0].trim(), cost);
            outputValue.set(cost);

            context.write(outputKey, outputValue);
        }
    }

    public static class SecondarySortReducer extends Reducer<CostBean, DoubleWritable, Text, DoubleWritable> {
        private final Text outputKey = new Text();
        private final DoubleWritable outputValue = new DoubleWritable();
        @Override
        protected void reduce(CostBean key, Iterable<DoubleWritable> values,Context context)
                throws IOException, InterruptedException {
            outputKey.set(key.getAccount());

            for (DoubleWritable v : values) {
                outputValue.set(v.get());
                context.write(outputKey, outputValue);
            }
        }
    }

    public int run(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3");
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, SecondarySortMapReduce.class.getSimpleName());
        job.setJarByClass(SecondarySortMapReduce.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //inputPath and outputPath
        FileSystem fs =FileSystem.get(conf);
        FileInputFormat.addInputPath(job, new Path("D:\\IdeaProjects\\HadoopDemo\\input"));
        Path outpath =new Path("D:\\IdeaProjects\\HadoopDemo\\output\\result");
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        // map settings
        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(CostBean.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // partition settings
        job.setPartitionerClass(CostBeanPatitioner.class);

        // sorting      
        job.setSortComparatorClass(CostBeanComparator.class);

        // grouping

        job.setGroupingComparatorClass(GroupComparator.class);

        // reduce settings
        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);

        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SecondarySortMapReduce(), args);
    }
}