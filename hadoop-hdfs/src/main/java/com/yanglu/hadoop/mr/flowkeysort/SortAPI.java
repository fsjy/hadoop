package com.yanglu.hadoop.mr.flowkeysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortAPI implements WritableComparable<SortAPI> {


    String pn;
    /**
     * 第一列数据
     */
    public Long first;
    /**
     * 第二列数据
     */
    public Long second;

    public SortAPI() {
    }

    public SortAPI(String pn, long first, long second) {
        this.pn = pn;
        this.first = first;
        this.second = second;
    }

    /**
     * 排序就在这里当：this.first - o.first > 0 升序，小于0倒序
     */
    @Override
    public int compareTo(SortAPI o) {
        long mis = (this.first - o.first)*-1;
        if (mis != 0) {
            return (int) mis;
        } else {
            return (int) (this.second - o.second);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pn);
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pn = in.readUTF();
        this.first = in.readLong();
        this.second = in.readLong();

    }

//    @Override
//    public int hashCode() {
//        return this.first.hashCode() + this.second.hashCode();
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (obj instanceof SortAPI) {
//            SortAPI o = (SortAPI) obj;
//            return this.first == o.first && this.second == o.second;
//        }
//        return false;
//    }

    @Override
    public String toString() {
        return "first: " + this.pn + " second: " + this.first + " third: " + this.second;
    }

}