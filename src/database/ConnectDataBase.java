package database;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectDataBase implements Runnable {// 代表一个任务，查询一个表，然后将结果写到txt中
    static Logger logger = LogManager.getLogger(ConnectDataBase.class);
    String tableName;
    File outFile = null;
    Statement sqlStatement = null;
    ResultSet resultSet = null;
    static final String SELECT_SQL = "SELECT top 10 * FROM  ";
    String tableName2 = "Group2";

    public ConnectDataBase(String threadName) {
	// outFile = new File(filePath);
	this.tableName = "Group" + threadName;
    }

    public ConnectDataBase(String threadName, File outfile,
	    Statement sqlStatement) {
	this(threadName);
	this.outFile = outfile;
	this.sqlStatement = sqlStatement;
    }

    @Override
    public void run() {
	// TODO Auto-generated method stub
	logger.warn("任务" + this.tableName + "运行任务中。。。线程id是："
		+ Thread.currentThread().getId());
	logger.warn("select字串： " + SELECT_SQL + tableName);
	// char b[] = new char[10 * 1024 * 1024];
	// Arrays.fill(b, tName);//用相同字符填充数组

	StringBuilder sbBuilder = new StringBuilder();
	FileWriter fWriter = null;
	try {
	    resultSet = sqlStatement.executeQuery(SELECT_SQL + tableName);
	    while (resultSet.next()) {
		sbBuilder.append(resultSet.getString(1) + ";" + "\r\n");
	    }
	    // Thread.sleep(1000);
	    char[] wr = new char[] { '0', '1', '2', '3', '4', '5' };
	    synchronized (outFile) {
		fWriter = new FileWriter(outFile, true);
		fWriter.write("线程" + tableName + "\r\n");
		fWriter.write(sbBuilder.toString());
		fWriter.write("\r\n");
		fWriter.close();
	    }
	} catch (IOException e) {
	    // TODO Auto-generated catch block
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
