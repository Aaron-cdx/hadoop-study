package com.duanxi.hbase.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * @author caoduanxi
 * @2019/10/6 21:33
 */
public class WCRunner {
    public static void main(String[] args) throws Exception {
        // 获取配置文件
        Configuration conf = new Configuration();
        // 获取配置中的文件
        conf.set("fs.defaultFS","hdfs://node01:8020");
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        // 获取工作对象
        Job job = Job.getInstance(conf);
        // 设置类
        job.setJarByClass(WCRunner.class);
        // 设置mapper
        job.setMapperClass(WCMapper.class);
        // 设置出去的key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        // 设置进入的文件路径
        FileInputFormat.addInputPath(job,new Path("/user/hive/warehouse/wc/"));
        // 设置reduce类
        TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null, null, null, null, false);

        job.waitForCompletion(true);
    }
}
