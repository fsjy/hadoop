package com.yanglu.hadoop.mr.flow.untity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 模拟log中流量的读取，采用FlowBean作为mr的发送给reduce的处理内容
 * <p></p>
 * mr中的key是手机号
 */
public class FlowBean implements Writable {

    private String phoneNum;

    private long up_flow;

    private long d_flow;

    private long s_flow;


    /**
     * 反序列化的场合需要一个空参的构造方法
     */
    public FlowBean() {
    }

    /**
     * 为对象数据初始化方便，加入一个带有参数的构造方法
     *
     * @param phoneNum
     * @param up_flow
     * @param d_flow
     */
    public FlowBean(String phoneNum, long up_flow, long d_flow) {

        this.phoneNum = phoneNum;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = up_flow + d_flow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {


        out.writeUTF(phoneNum);
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(s_flow);


    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     * <p>
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     * <p>
     * _/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
     * <p>
     * 读数据的场合必须和序列化时的顺序保持一致
     * <p>
     * _/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        this.phoneNum = in.readUTF();
        this.up_flow = in.readLong();
        this.d_flow = in.readLong();
        this.s_flow = in.readLong();

    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {

        return "Summary is : " + String.valueOf(this.s_flow);

    }


}