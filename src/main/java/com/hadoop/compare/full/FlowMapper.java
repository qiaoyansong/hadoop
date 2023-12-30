package com.hadoop.compare.full;

import com.hadoop.compare.full.vo.MapperFlowVO;
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
public class FlowMapper extends Mapper<LongWritable, Text, MapperFlowVO, Text> {

    private MapperFlowVO outK = new MapperFlowVO();

    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取一行数据,转成字符串
        String line = value.toString();
        //2 切割数据
        String[] split = line.split("\t");
        //3 抓取我们需要的数据:手机号,上行流量,下行流量
        String phone = split[0];
        String up = split[1];
        String down = split[2];
        String sum = split[3];
        //4 封装 outK outV
        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow(Long.parseLong(sum));

        outV.set(phone);

        //5 写出 outK outV
        context.write(outK, outV);
    }

}
