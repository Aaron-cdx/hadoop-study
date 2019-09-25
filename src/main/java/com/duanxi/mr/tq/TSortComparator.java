package com.duanxi.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序器
 *
 * @author caoduanxi
 * @2019/9/25 13:53
 */
public class TSortComparator extends WritableComparator {

    // 构造器，调用父类的方法生成比较的实例
    public TSortComparator() {
        super(TQ.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 转换成天气做对比，此时需要比较的是三个维度
        TQ t1 = (TQ) a;
        TQ t2 = (TQ) b;

        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
            if (c2 == 0) {
                // 如果是同一天的话，采用逆序排列
                return -Integer.compare(t1.getWd(), t2.getWd());
            }
        }
        return c1;
    }
}
