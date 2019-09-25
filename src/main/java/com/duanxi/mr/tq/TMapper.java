package com.duanxi.mr.tq;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author caoduanxi
 * @2019/9/25 10:49
 */
public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable> {
    TQ mkey = new TQ();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 主要是设置mkey-mval，获取到分区的数据      2019-09-20 12:20:31    34c
        try {
            String[] str = StringUtils.split(value.toString(), "\t");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date time = sdf.parse(str[0]);
            // 获取到时间的数据
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(time);

            mkey.setYear(calendar.get(Calendar.YEAR));
            // 看注解可以发现这个月份是从1开始，但是值是从0开始
            mkey.setMonth(calendar.get(Calendar.MONTH) + 1);
            mkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));

            // 取出温度
            int wd = Integer.parseInt(str[1].substring(0, str[1].length() - 1));
            mkey.setWd(wd);

            mval.set(wd);

            context.write(mkey, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
