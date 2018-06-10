
package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ToHBase {

    static Logger logger = LogManager.getLogger(ToHBase.class);

    static String tableNameStr = "javatest";
    static String cfName = "qqnum";

    static void conHbase() {
	Configuration.addDefaultResource("./conf/hbase-site.xml");
	Configuration conf = HBaseConfiguration.create();
	// 在eclipse中运行，下面两句很重要，也表现出：client是通过与zookeeper直接交互实现和hbase连接的。zookeeper是hbase的门卫
	conf.set("hbase.zookeeper.quorum", "master");
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	try {
	    Connection connection = ConnectionFactory.createConnection(conf);
	    Admin admin = connection.getAdmin();
	    HTableDescriptor tableDes = new HTableDescriptor(
		    TableName.valueOf(tableNameStr)); // connection.getTable();
	    HColumnDescriptor cfDes = new HColumnDescriptor(cfName);
	    tableDes.addFamily(cfDes);
	    HTableDescriptor[] tables = admin.listTables();

	    if (tables.length > 1 && Bytes.equals(tableNameStr.getBytes(),
		    tables[0].getTableName().getName())) {
		logger.debug(tables[1].getNameAsString());
		logger.debug(tables.length);
		logger.debug(tableNameStr + "表已经存在");
		// throw new IOException("无法新建表");
	    } else {
		// admin.createTable(tableDes);// 真正建表
	    }
	    Table table = connection.getTable(TableName.valueOf(tableNameStr));
	    try {

		for (int i = 0; i < 5; i++) {
		    byte[] rowName = Bytes.toBytes("rowkey" + i);
		    Put putCell = new Put(rowName);
		    byte[] cf = Bytes.toBytes(cfName);
		    byte[] columnName = Bytes.toBytes("c" + String.valueOf(i));
		    byte[] value = Bytes.toBytes("value" + i);
		    putCell.add(cf, columnName, value);
		    table.put(putCell);
		    // putCell.toJSON();
		}
		// Json avroJson;
		Get get = new Get(Bytes.toBytes("rowkey2"));
		Result result = table.get(get);
		logger.info("get rowkey2的结果： " + result.toString());
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		try {
		    for (Result res : resultScanner) {
			// Result res = resultScanner.next();
			logger.debug("scan遍历表的结果：" + new String(res.getRow())
				+ res.getRow().toString());
			logger.debug("scan遍历表的结果：" + new String(res.value()));
			for (KeyValue kv : res.raw()) {

			    logger.debug("scan遍历表的结果：" + kv.toString()
				    + kv.getRow());
			}

		    }
		    // String jsonRes = get.toJSON();
		    // logger.debug("结果是：", jsonRes);
		    logger.info("get 还是原来的Get类型吗？？ ");
		    logger.info(get instanceof Get);

		} finally {
		    // TODO: handle exception
		    resultScanner.close();
		}
		// admin.disableTable(TableName.valueOf(tableNameStr));
		// admin.deleteTable(TableName.valueOf(tableNameStr));

	    } finally {
		table.close();
		admin.close();
		connection.close();
	    }
	} catch (IOException e) {
	    // TODO 自动生成的 catch 块
	    e.printStackTrace();
	} finally {

	}
    }

    public static void main(String[] args) {
	logger.info("准备写入HBase：");
	conHbase();
    }
}
