package com.duanxi.mr.tq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author caoduanxi
 * @2019/9/25 11:07
 */
public class TQ implements WritableComparable<TQ> {
    private int year;
    private int month;
    private int day;
    private int wd;

    public TQ() {
    }

    public TQ(int year, int month, int day, int wd) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.wd = wd;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    // 日期正序
    @Override
    public int compareTo(TQ that) {
        int c1 = Integer.compare(this.year, that.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(this.month, that.getMonth());
            if (c2 == 0) {
                return Integer.compare(this.day, that.getDay());
            }
            return c2;
        }
        return c1;
    }

    // 下面两个方法是序列化与反序列化
    // 写的时候也需要按照整型写入否则会出现I/O异常！
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
    }
}
