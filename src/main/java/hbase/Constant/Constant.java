package hbase.Constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Klein
 * @Package hbase.students
 * @Description: students服务
 * @date 17/6/18 11:36
 */
public interface Constant {
    String TABLE_NAME = "t_students";
    String STU_ROW_NAME = "stu_row1";
    byte[] FAMILY_NAME_1 = Bytes.toBytes("cf1");
    byte[] FAMILY_NAME_2 = Bytes.toBytes("cf2");
    byte[] FAMILY_NAME_3 = Bytes.toBytes("cf3");
    byte[] CLOUMN1 = Bytes.toBytes("name");
    byte[] CLOUMN2 = Bytes.toBytes("age");
    byte[] CLOUMN3 = Bytes.toBytes("score");
}
