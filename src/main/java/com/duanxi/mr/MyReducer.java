package com.duanxi.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/9/23 11:36
 */
// 原语：一个组的数据对应一个reduce，由一个reduce计算
    // 相同的key为一组，调用一次reduce方法，在方法内迭代这一组数据，进行计算
public class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
