package com.duanxi.mr.fof;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/9/26 9:53
 */
public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text fKey = new Text();
    IntWritable fVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // map的主要功能就是实现将输入的数据映射称为k&v&p键值对
        // 首先对数据进行一个切割
        // om hadoop cat world
        String[] strs = StringUtils.split(value.toString(), ' ');
        // 数据的组合，获取所有的直接关系与间接关系
        for (int i = 1; i < strs.length; i++) {
            fKey.set(getFof(strs[0], strs[i]));
            // 设置的直接关系
            fVal.set(0);
            context.write(fKey, fVal);
            // 设置间接关系
            for (int j = i + 1; j < strs.length; j++) {
                fKey.set(getFof(strs[i], strs[j]));
                fVal.set(1);
                context.write(fKey, fVal);
            }
        }
    }

    public static String getFof(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }
        return s2 + ":" + s1;
    }
}
