package com.hadoop.flow.vo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/29 4:08 下午
 * description：输入数据的vo对象
 */
public class MapperFlowVO implements Writable {
    /**
     * 上行流量
     */
    private long upFlow;

    /**
     * 下行流量
     */
    private long downFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long donwFlow) {
        this.downFlow = donwFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setUpFlow(dataInput.readLong());
        this.setDownFlow(dataInput.readLong());
    }
}
