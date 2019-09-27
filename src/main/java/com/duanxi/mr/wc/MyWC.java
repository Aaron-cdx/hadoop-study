package com.duanxi.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author caoduanxi
 * @2019/9/23 11:23
 */
// 用作单词统计
public class MyWC {
    // 客户端
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        // 强转获取job的配置信息
        JobConf jobConf = (JobConf) job.getConfiguration();
        job.setJobName("wordCount");
        job.setJarByClass(MyWC.class);
        // 设置文件的输入路径
        Path input = new Path("/user/root/sort/txt");
        FileInputFormat.setInputPaths(job, input);
        // 设置文件的输出路径，由于hadoop规定不允许输出目录为已存在的目录
        // 所以需要校验输出的文件目录是否存才，如果存在的话需要先删除
        Path output = new Path("/data/wc/output");
        // 从配置中获取到文件系统
        if (output.getFileSystem(conf).exists(output)) {
            // 保证递归删除
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(99);
        job.setReducerClass(MyReducer.class);
        job.waitForCompletion(true);

    }
}
