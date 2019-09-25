package com.duanxi.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author caoduanxi
 * @2019/9/25 10:40
 */
// 主要实现找出每个月气温最高的两天
public class MyTQ {

    public static void main(String[] args) throws Exception {
        // 加载配置
        Configuration conf = new Configuration(true);
        // 获取工作
        Job job = Job.getInstance(conf);
        // 设置jar类，方便寻找
        job.setJarByClass(MyTQ.class);
        // conf 开始配置

        // 输入格式化类，如果需要可以自己定义
        // job.setInputFormatClass(ooxx.class);

        // 设置map，获取mapper
        job.setMapperClass(TMapper.class);
        // 设置输出的key-value键值对类型
        job.setOutputKeyClass(TQ.class);
        job.setOutputValueClass(IntWritable.class);
        // 由于map的主要功能就是映射数据形成keyvalue，需要设置比较器
        job.setSortComparatorClass(TSortComparator.class);
        // 在输出的时候需形成k&v&p，所以需要设置分区
        job.setPartitionerClass(TPartitioner.class);
        // 输出之后需要进入溢写区，按照框架给的默认的即可

        // 设置combiner，如果有需要数据合并的话
        // job.setCombinerClass(TCombiner.class);

        // 设置reduce，获取reduce
        job.setReducerClass(TReduce.class);
        // 使用reduce:原语：一组数据对应一个reduce，在一个reduce方法中迭代数据，进行计算
        // 设置分组比较器
        job.setGroupingComparatorClass(TGroupingComparator.class);
        // 设置reduce个数
        job.setNumReduceTasks(2);

        // 设置文件输入路径
        Path input = new Path("/data/tq/input");
        FileInputFormat.addInputPath(job, input);
        // 设置文件输出路径
        Path output = new Path("/data/tq/output");
        // 由于hadoop对于文件输出的路径要求不能已存在的
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }

    /*public static void main(String[] args) throws Exception {
        // 加载配置
        Configuration conf = new Configuration(true);
        // 获取工作实例
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTQ.class);
        // 设置输入格式化
        // job.setInputFormatClass(ooxx.class);

        // -------开始处理map-------
        // 设置mapper处理类
        job.setMapperClass(TMapper.class);


        // 设置输出键值对的格式化
        job.setOutputKeyClass(TQ.class);
        job.setOutputValueClass(IntWritable.class);
        // 生成分区
        job.setPartitionerClass(TPartitioner.class);

        // 需要给出比较器
        job.setSortComparatorClass(TSortComparator.class);

        // 设置合并器
//        job.setCombinerClass(TCombiner.class);

        // -------map处理结束-------

        // -------reduce处理--------
        // reduce拉取数据:分组比较器
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReduce.class);

        // -------reduce处理结束------

        // 设置文件输入流的路径
        Path input = new Path("/data/tq/input");
        FileInputFormat.addInputPath(job,input);
        // 设置输出文件
        Path output = new Path("/data/tq/output");
        // 需要判断文件路径是否存在，存在的话就删除
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);
        // 设置处理任务的reduce数为2个
        job.setNumReduceTasks(2);

        // 等待完成
        job.waitForCompletion(true);
    }*/
}
