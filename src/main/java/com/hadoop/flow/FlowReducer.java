package com.hadoop.flow;

import com.hadoop.flow.vo.MapperFlowVO;
import com.hadoop.flow.vo.ReducerFlowVO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 4:22 下午
 * description：
 * KEYIN, map阶段输出的key类型，Text
 * VALUEIN,map阶段输入的value类型, MapperFlowVO
 * KEYOUT,reduce阶段输出的key类型，Text 这里代表的是手机号
 * VALUEOUT，reduce阶段输入的value类型, ReducerFlowVO
 */
public class FlowReducer extends Reducer<Text, MapperFlowVO, Text, ReducerFlowVO> {

    private ReducerFlowVO outV = new ReducerFlowVO();

    @Override
    protected void reduce(Text key, Iterable<MapperFlowVO> values, Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        //1 遍历 values,将其中的上行流量,下行流量分别累加
        for (MapperFlowVO flowBean : values) {
            totalUp += flowBean.getUpFlow();
            totalDown += flowBean.getDownFlow();
        }
        //2 封装 outKV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        //3 写出 outK outV
        context.write(key, outV);
    }

}
