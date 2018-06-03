package database;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DBMainClass {
    static Logger logger = LogManager.getLogger(DBMainClass.class);
    static final String OUT_FILENAME = "./data/b.txt";
    static boolean starting = true;

    static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static final String DB_IP = "112.24.14.203";
    static final String DataBaseNAME = "GroupData1";

    static final String DB_URL = "jdbc:sqlserver://112.24.14.203:1433; DatabaseName=";
    static final String DB_USER = "sa";
    static final String DB_PWD = "123456";
    static final String DB_SELECT_SQL = "SELECT top 10 * FROM Group2";
    // Declare the JDBC objects.
    static Connection connection = null;
    static Statement sqlStatement = null;
    static ResultSet resultSet = null;

    File outFile = null;
    static BlockingQueue<Runnable> taskQuene = new LinkedBlockingQueue<>(5); // 生产者队列

    DBMainClass(String filePath) {
	outFile = new File(filePath);
    }

    void initConn() {
	try {
	    Class.forName(DRIVER_CLASS);
	    connection = DriverManager.getConnection(DB_URL + DataBaseNAME,
		    DB_USER, DB_PWD);
	    sqlStatement = connection.createStatement();
	    // resultSet = sqlStatement.executeQuery(DB_SELECT_SQL);

	} catch (Exception e) {
	    // TODO: handle exception
	    e.printStackTrace();
	}
    }

    void writeResult() {
	StringBuilder sBuilder = new StringBuilder();
	FileWriter fWriter = null;
	try {
	    fWriter = new FileWriter(outFile);
	    fWriter.write(sBuilder.toString());
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    static void useThreadPool() {
	logger.info("主函数,线程id：" + Thread.currentThread().getId());
	ThreadPoolExecutor taskPools = new ThreadPoolExecutor(4, 8, 30,
		TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(3));// 消费者队列
	logger.warn("创建线程池" + taskQuene.isEmpty());

	DBMainClass testClass = new DBMainClass(OUT_FILENAME);
	Thread produce = new Thread() {// 开启一个生产者线程往队列里面放task
	    public void run() {
		for (int i = 0; i < 15; i++) {
		    try {
			logger.info(i + "向队列生产/添加任务，生产者线程id："
				+ Thread.currentThread().getId());

			taskQuene.put(new ConnectDataBase(String.valueOf(i)));

		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }

		}
	    };
	};
	produce.start();

	while (starting) {
	    if (taskPools.getQueue().remainingCapacity() > 0) {
		Runnable currenTask;
		try {
		    currenTask = taskQuene.poll(100, TimeUnit.MILLISECONDS);
		    if (currenTask != null) {
			logger.fatal("从消费队列弹出任务："
				+ Thread.currentThread().getId());
			taskPools.execute(currenTask);
		    }
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

	    }
	    if ((!produce.isAlive()) && taskQuene.isEmpty()
		    && taskPools.getQueue().isEmpty()) { // 当生产者死亡，生产者的队列空了，线程池的队列也空了，就说明所以任务完成了。
		starting = false; // 用来结束main线程的while循环
		taskPools.shutdown();// 用来关闭消费者线程池
		logger.info("已经关闭消费者线程池。");
	    }
	}

    }

    public static void main(String[] args) throws InterruptedException {
	// useThreadPool();
	DBMainClass testClass = new DBMainClass(OUT_FILENAME);
	ThreadPoolExecutor pools = new ThreadPoolExecutor(4, 8, 30,
		TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(4));
	testClass.initConn();
	int i = 1;
	char[] wr = new char[] { '0', '1', '2', '3', '4', '5' };

	while (i < 6) {// 循环查询数据库创建任务
	    if (pools.getQueue().remainingCapacity() > 0) {
		logger.info("添加任务" + i);

		pools.execute(new ConnectDataBase(String.valueOf(i),
			testClass.outFile, DBMainClass.sqlStatement));
		i++;
	    } else {
		// Thread.sleep(100);
		continue;
	    }

	}
	logger.warn("程序结束");
	while (true) {
	    if (pools.getQueue().isEmpty()) {
		pools.shutdown();
		logger.info("已经关闭消费者线程池。");
		if (pools.isTerminated()) {
		    logger.info("所有任务已经执行完毕，关闭数据库连接。");

		    if (sqlStatement != null)
			try {
			    sqlStatement.close();
			} catch (Exception e) {
			}
		    if (connection != null)
			try {
			    connection.close();
			} catch (Exception e) {
			}
		    return;
		}
	    }
	}
    }
}
