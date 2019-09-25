package com.duanxi.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器，只有两个维度，判断是否是一组即可
 *
 * @author caoduanxi
 * @2019/9/25 14:04
 */
public class TGroupingComparator extends WritableComparator {
    public TGroupingComparator(){
        super(TQ.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ) a;
        TQ t2 = (TQ) b;
        // 按照年月分组，取的温度值
        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0) {
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return c1;
    }
}
