package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MRJobMainDriver extends Configured implements Tool {
    static Logger logger = LogManager.getLogger(MRJobMainDriver.class);
    static {
	System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
	System.setProperty("HADOOP_USER_NAME", "hadoop");
	Configuration.addDefaultResource("./conf/hadoop-local.xml");
	// Configuration.addDefaultResource("./conf/hadoop-cluster.xml");
	// Configuration.addDefaultResource("./conf/hadoop-xuniji.xml");
    }

    public int run(String[] folders) throws Exception {
	Configuration conf = new Configuration();
	// conf.addResource("./conf/hadoop-local.xml");
	// System.setProperty("HADOOP_HOME", "D:\\hadoop-2.6.0");
	String s1 = conf.get("dfs.namenode.name.dir");
	String s2 = conf.get("hadoop.tmp.dir");
	logger.info("namenode.name.dir ： " + s1);
	logger.info("hadoop.tmp.dir ： " + s2);

	Job job = Job.getInstance(conf);
	job.setJobName("qq群处理");
	job.setJarByClass(MRJobMainDriver.class);
	job.setMapperClass(QunMapper.class);
	job.setCombinerClass(QunReducer.class);
	job.setReducerClass(QunReducer.class);

	// 设置job的输出文件格式,也就是reduce端的KV输出类型
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(UserInfo.class);
	// job.setOutputValueClass(IntWritable.class);

	// job.setNumReduceTasks(3); // 设置reducetask个数
	this.testDFS(folders[1]);
	FileInputFormat.addInputPath(job, new Path(folders[0]));
	FileOutputFormat.setOutputPath(job, new Path(folders[1]));

	return job.waitForCompletion(true) ? 1 : 0;

    }

    public void testDFS(String folder) throws Exception {
	FileSystem myhdfs = FileSystem.get(new Configuration());
	Path outpath = new Path(folder);
	if (myhdfs.exists(outpath)) {
	    myhdfs.delete(outpath, true);
	    logger.info("已经删除原来的输出目录:" + folder);
	} else {
	    logger.debug("输出目录已经不存在。");
	    logger.debug(myhdfs.getName()
		    + myhdfs.getHomeDirectory().toString());
	}

    }

    public static void main(String[] args) throws Exception {
	// TODO 自动生成的方法存根
	logger.debug("This is MR驱动程序：");
	String[] folders = new String[] {
		"E:/qqData/GroupDatas-完整的6列/GroupData11/*", "./data/output" };// 程序内置输入输出文件夹
	folders = new String[] { "./data/aaa.txt", "./data/output" };
	// folders = args;// 获取用户输入的文件夹；默认根路径 hdfs：/user/hadoop
	if (folders.length != 2) {
	    logger.debug("提示，程序运行格式为: wordcount <inpath> <outpath>");
	    System.exit(2);
	} else {
	    logger.info("<inpath>：" + folders[0] + " <outpath>：" + folders[1]);
	}
	// TODO 自动生成的方法存根
	MRJobMainDriver driver = new MRJobMainDriver();
	driver.testDFS(folders[1]);

	int exitcode = 0;
	exitcode = ToolRunner.run(new MRJobMainDriver(), folders);
	logger.fatal("退出程序！！！");
	logger.fatal("总共处理了多少行：" + QunMapper.allRow);
	logger.fatal(" 总共有多少不同的key：" + QunReducer.result);
	System.exit(exitcode);
    }
}
