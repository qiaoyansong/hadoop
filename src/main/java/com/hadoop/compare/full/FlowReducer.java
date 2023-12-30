package com.hadoop.compare.full;

import com.hadoop.compare.full.vo.MapperFlowVO;
import com.hadoop.compare.full.vo.ReducerFlowVO;
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
public class FlowReducer extends Reducer<MapperFlowVO, Text, Text, ReducerFlowVO> {

    private ReducerFlowVO outV = new ReducerFlowVO();

    /**
     * 因为mapper的key 是MapperFlowVO，因此相同key 会调用一次，这里的values实际上是手机号
     */
    @Override
    protected void reduce(MapperFlowVO key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历 values 集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            //调换 KV 位置,反向写出
            outV.setUpFlow(key.getUpFlow());
            outV.setDownFlow(key.getDownFlow());
            context.write(value, outV);
        }
    }

}
