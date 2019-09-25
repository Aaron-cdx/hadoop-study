package com.duanxi.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/9/25 14:06
 */
public class TReduce extends Reducer<TQ, IntWritable, Text, IntWritable> {
    // 构建输出数据，都需要写入到上下文中
    Text reduceKey = new Text();
    IntWritable reduceVal = new IntWritable();
    @Override
    protected void reduce(TQ key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int data = 0;
        int day = 0;
        // 对真迭代器中的数据做一个遍历
        for (IntWritable value : values) {
            // 表示是第一个数据
            if(data == 0){
                // 写入处理
                reduceKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                reduceVal.set(key.getWd());
                data++;
                day = key.getDay();
                // 写入上下文
                context.write(reduceKey,reduceVal);
            }
            // 数据迭代更新,找到了第二高的数据了
            if(data != 0 && day != key.getDay()){
                reduceKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                reduceVal.set(key.getWd());
                context.write(reduceKey,reduceVal);
                break;
            }
        }
    }
}
