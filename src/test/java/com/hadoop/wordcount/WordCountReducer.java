package com.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 1:30 下午
 * description：
 * KEYIN, map阶段输出的key类型，Text
 * VALUEIN,map阶段输入的value类型, IntWritable
 * KEYOUT,reduce阶段输出的key类型，Text
 * VALUEOUT，reduce阶段输入的value类型, IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int sum;
    private IntWritable v = new IntWritable();

    /**
     * key对应KEYIN,即map阶段输出的key
     * value对应VALUEIN，代表map阶段当前key对应的所有value值
     * org.apache.hadoop.mapreduce.Reducer#run(org.apache.hadoop.mapreduce.Reducer.Context) 调用
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key, v);
    }
}
