package com.duanxi.hbase.wc;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/10/6 21:57
 */
public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 需要放入rowkey，其中rowkey直接使用单词来的key就行
        Put put = new Put(key.toString().getBytes());
        put.addColumn("cf".getBytes(),"cnum".getBytes(), Bytes.toBytes(sum));
        context.write(null,put);
    }
}
