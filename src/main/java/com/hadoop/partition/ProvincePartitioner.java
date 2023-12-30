package com.hadoop.partition;

import com.hadoop.flow.vo.MapperFlowVO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 9:06 下午
 * description：
 * KEY 为 Mapper 输出的key
 * VALUE 为 Mapper 输出的value类型
 */
public class ProvincePartitioner extends Partitioner<Text, MapperFlowVO> {

    @Override
    public int getPartition(Text text, MapperFlowVO mapperFlowVO, int numPartitions) {
        //获取手机号前三位 prePhone
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        //定义一个分区号变量 partition,根据 prePhone 设置分区号
        int partition;
        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }
        //最后返回分区号 partition
        return partition;
    }
}
