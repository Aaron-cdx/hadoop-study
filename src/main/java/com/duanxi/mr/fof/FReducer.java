package com.duanxi.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/9/26 10:01
 */
public class FReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // 能保证key一定是唯一的，如果相同的key就作为一组，所以不用重新设定key
    IntWritable val = new IntWritable();

    /**
     * 注意，在reduce中，与run方法是存在一个来回调用的过程，所以reduce方法
     * 会与生成的组个数一样，多少组就调用多少次
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        // 关联的好友数量，只关关心获取到的间接关系
        int sum = 0;
        for (IntWritable value : values) {
            // 表名直接关系
            if (value.get() == 0) {
                flag = 1;
            }
            sum += value.get();
        }
        if (flag == 0) {
            val.set(sum);
            context.write(key, val);
        }
    }
}
