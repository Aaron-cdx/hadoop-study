package com.duanxi.mr.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 第一个MR，计算TF和计算N(微博总数)
 * 
 * @author root
 *
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// map的职责就是映射数据生成键值对，可以生成任意形式的键值对，只要能够放入
	// reduce中去做方法的处理和调用即可。
	// reduce中按照原语格式来的话，相同的key为一组进行一次reduce方法的迭代计算。
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//3823890210294392	今天我约了豆浆，油条
		String[] v = value.toString().trim().split("\t");
		
		if (v.length >= 2) {
			
			String id = v[0].trim();
			String content = v[1].trim();

			StringReader sr = new StringReader(content);
			IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
			Lexeme word = null;
			while ((word = ikSegmenter.next()) != null) {
				String w = word.getLexemeText();
				// 组合词用户名作为键值对传入
				context.write(new Text(w + "_" + id), new IntWritable(1));
				//今天_3823890210294392	1
			}
			// 将总数存入
			context.write(new Text("count"), new IntWritable(1));
			//count 1
			
		} else {
			System.out.println(value.toString() + "-------------");
		}
	}

}
