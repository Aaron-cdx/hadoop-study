package com.duanxi.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区器
 *
 * @author caoduanxi
 * @2019/9/25 13:52
 */
public class TPartitioner extends Partitioner<TQ, IntWritable> {

    @Override
    public int getPartition(TQ tq, IntWritable intWritable, int numPartitions) {
        return tq.hashCode() % numPartitions;
    }
}
