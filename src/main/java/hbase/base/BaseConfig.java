package hbase.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

/**
 * @author klein
 * @Package hbase.base
 * @Description:
 * @date 17/3/18 10:59
 */
public class BaseConfig {

    /**
     * 创建hbase连接
     * @return
     */
    public static Connection getConnection() throws Exception{
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "myhbase");
//        conf.set("hbase.master", "192.168.44.129:9000");
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

    public static void main(String[] args) throws Exception {
        Admin admin = getConnection().getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("TEST"));
        HColumnDescriptor column = new HColumnDescriptor("COLUMN");
        tableDescriptor.addFamily(column);
        //添加缓存
        column.setBlockCacheEnabled(true);
        //添加内存
        column.setInMemory(true);
        //设置最大版本数
        column.setMaxVersions(1);

        admin.createTable(tableDescriptor);

        admin.close();
        System.out.println("args = [" + getConnection() + "]");
    }
}
