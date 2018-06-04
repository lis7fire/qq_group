package database;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectDataBase implements Runnable {// 代表一个任务，查询一个表，然后将结果写到txt中
    static Logger logger = LogManager.getLogger(ConnectDataBase.class);
    String tableName;
    File outFile = null;
    static Connection connection = null; // 必须是静态全局的，一遍多线程共享
    int rowNum = 0;
    Statement sqlStatement = null;
    ResultSet resultSet = null;
    static final String SELECT_SQL = "SELECT [QQNum],[Nick],[Age],[Gender],[Auth],[QunNum] FROM ";
    // static final String SELECT_SQL = "SELECT * FROM ";

    public ConnectDataBase(String threadName) {
	this.tableName = threadName;
    }

    public ConnectDataBase(String threadName, File outfile,
	    Connection connection) {
	this(threadName);
	this.outFile = outfile;
	ConnectDataBase.connection = connection;
    }

    @Override
    public void run() {
	// TODO Auto-generated method stub
	synchronized (connection) {
	    try {
		this.sqlStatement = connection.createStatement();
		this.resultSet = this.sqlStatement
			.executeQuery(SELECT_SQL + tableName);
	    } catch (SQLException e) {
		// TODO 自动生成的 catch 块
		e.printStackTrace();
	    }
	}
	logger.info("任务" + this.tableName + "运行任务中。。。线程id是："
		+ Thread.currentThread().getId());
	logger.info(this.tableName + "select字串： " + SELECT_SQL + tableName);
	// char b[] = new char[10 * 1024 * 1024];
	// Arrays.fill(b, tName);//用相同字符填充数组

	StringBuilder sbBuilder = new StringBuilder();
	FileWriter fWriter = null;
	try {
	    while (resultSet.next()) {
		sbBuilder.append(resultSet.getString(1) + ";"
			+ resultSet.getString(2) + ";" + resultSet.getString(3)
			+ ";" + resultSet.getString(4) + ";"
			+ resultSet.getString(5) + ";" + resultSet.getString(6)
			+ "\r\n");
		rowNum++;
	    }
	    logger.info(this.tableName + "游标行号:" + rowNum);
	    // Thread.sleep(1000);
	    // char[] wr = new char[] { '0', '1', '2', '3', '4', '5' };
	    synchronized (outFile) {
		fWriter = new FileWriter(outFile, true);
		// fWriter.write("线程" + tableName + "\r\n");
		fWriter.write(sbBuilder.toString());
		fWriter.write("\r\n");
		fWriter.close();
	    }

	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    logger.fatal(this.tableName + "IO报错");
	    e.printStackTrace();
	} catch (Exception e) {
	    // TODO: handle exception
	    logger.fatal(this.tableName + "报错");
	    e.printStackTrace();
	} finally {
	    logger.fatal(this.tableName + "开始关闭结果集");

	    if (resultSet != null)
		try {
		    resultSet.close();
		} catch (Exception e) {
		}
	}

	logger.info("任务" + this.tableName + "运行结束，id是："
		+ Thread.currentThread().getId());
    }
}
