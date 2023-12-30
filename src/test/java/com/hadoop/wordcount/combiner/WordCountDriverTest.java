package com.hadoop.wordcount.combiner;

import com.hadoop.wordcount.WordCountMapper;
import com.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 1:39 下午
 * description：
 */
public class WordCountDriverTest {

    @Test
    public void testDriver() throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本 Driver 程序的 jar
        job.setJarByClass(WordCountDriverTest.class);

        // 3 关联 Mapper 和 Reducer 的 jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountCombiner.class);
        // 如果reduceTask为0 那么就代表没有reduce阶段，即只有mapper阶段，
        // combiner不生效(因为combiner是shuffle阶段的)
//        job.setNumReduceTasks(0);
        // 发现对于这个场景combiner与reducer逻辑完全一致，直接使用Reducer类进行combiner也是可以的
//        job.setCombinerClass(WordCountReducer.class);

        String srcPath = "/Users/didi/Desktop/markdown/hadoop/test/wordCount/input/test.txt";
        String destPath = "/Users/didi/Desktop/markdown/hadoop/test/wordCount/combiner/output/";

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));

        // 7 提交 job
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
