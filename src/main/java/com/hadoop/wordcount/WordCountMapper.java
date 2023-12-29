package com.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 10:44 上午
 * description：
 * KEYIN, map阶段输入的key类型，这里的是固定的 代表文件的偏移量，LongWritable
 * VALUEIN,map阶段输入的value类型, 看你输入的文件数据的类型，这里的文件是文本文件，因此就是Text
 * KEYOUT,map阶段输出的key类型，Text
 * VALUEOUT，map阶段输入的value类型, IntWritable
 * 可以看上面的图 map阶段，这里的KEYOUT VALUEOUT，就是map阶段最后输出的k-v对
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();

    private IntWritable v = new IntWritable(1);

    /**
     * key对应KEYIN,即偏移量
     * value对应VALUEIN，代表输入文件的一行
     * 会被org.apache.hadoop.mapreduce.Mapper#run(org.apache.hadoop.mapreduce.Mapper.Context) 调用
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" "); // atguigu atguigu
        // 3 输出
        for (String word : words) {
            // k v声明放到最外层，防止频繁的创建对象，与此同时因为Mapper#run 是循环串行调用的不用考虑线程安全问题
            k.set(word);
            // 这里的k -v 为输出的KEYOUT VALUEOUT
            // atguigu 1
            // atguigu 1
            context.write(k, v);
        }
    }
}
