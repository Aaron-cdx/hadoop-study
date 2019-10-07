package com.duanxi.hbase.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author caoduanxi
 * @2019/10/6 21:35
 */
public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // map负责的是数据映射写入到context中去
        // 分割获取数据
        String[] split = value.toString().split(" ");
        for (String s : split) {
            context.write(new Text(s),new IntWritable(1));
        }

        /*StringTokenizer string = new StringTokenizer(value.toString());
        while(string.hasMoreTokens()){
            context.write(new Text(string.nextToken()),new IntWritable(1));
        }*/
    }
}
