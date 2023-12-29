package com.hadoop.flow;

import com.hadoop.flow.vo.MapperFlowVO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 4:16 下午
 * description：
 * KEYIN, map阶段输入的key类型，这里的是固定的 代表文件的偏移量，LongWritable
 * VALUEIN,map阶段输入的value类型, 看你输入的文件数据的类型，这里的文件是文本文件，因此就是Text
 * KEYOUT,map阶段输出的key类型，Text 手机号
 * VALUEOUT，map阶段输出的value类型, MapperFlowVO
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, MapperFlowVO> {

    private Text outK = new Text();

    private MapperFlowVO outV = new MapperFlowVO();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取一行数据,转成字符串
        String line = value.toString();
        //2 切割数据
        String[] split = line.split("\t");
        //3 抓取我们需要的数据:手机号,上行流量,下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        //4 封装 outK outV
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));

        //5 写出 outK outV
        context.write(outK, outV);
    }

}
