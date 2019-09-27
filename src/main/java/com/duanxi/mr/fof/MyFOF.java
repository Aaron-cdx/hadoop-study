package com.duanxi.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 计算好友之间的间接好友，完成好友推荐的程序编写
 *
 * @author caoduanxi
 * @2019/9/26 9:36
 */
public class MyFOF {

    public static void main(String[] args) throws Exception {
        // 获取配置
        Configuration conf = new Configuration(true);
        // 获取工作
        Job job = Job.getInstance(conf);
        // 设施jarclass
        job.setJarByClass(MyFOF.class);
        // map
        // 输入格式化，使用默认的
        // 获取mapper
        job.setMapperClass(FMapper.class);
        // 获取输出的key&value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 分区器（由于默认使用的是key的hashcode模值）
        // 比较器的话使用默认的
        // 合并器，就不设置了


        // reduce 分组器也不设置了
        job.setReducerClass(FReducer.class);

        // 输入输出文件路径

        Path input = new Path("/data/fof/input");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/data/fof/output");
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

    }
}
