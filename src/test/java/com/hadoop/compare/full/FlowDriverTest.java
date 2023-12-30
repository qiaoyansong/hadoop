package com.hadoop.compare.full;

import com.hadoop.compare.full.vo.MapperFlowVO;
import com.hadoop.compare.full.vo.ReducerFlowVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/30 2:13 下午
 * description：
 */
public class FlowDriverTest {

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本 Driver 程序的 jar
        job.setJarByClass(FlowDriverTest.class);

        // 3 关联 Mapper 和 Reducer 的 jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4 设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(MapperFlowVO.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ReducerFlowVO.class);

        // 获取flow output的输入数据
        String srcPath = "/Users/didi/Desktop/markdown/hadoop/test/flow/output/part-r-00000";
        String destPath = "/Users/didi/Desktop/markdown/hadoop/test/flow/compare/full/output/";

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));

        // 7 提交 job
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
