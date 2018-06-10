package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QunReducer extends Reducer<Text, UserInfo, Text, UserInfo> {
    static Logger logger = LogManager.getLogger(QunReducer.class);
    IntWritable res = new IntWritable();
    static int result = 0;

    protected void reduce(Text key, Iterable<UserInfo> values, Context context)
	    throws IOException, InterruptedException {

	result++;// red共有多少组的输入，也就是总共有多少不同的key
	int sum = 0;
	UserInfo userInfo = null;// 保存一个key的所有值得最终输出结果
	// GenericRecord user = null;
	// TODO 自动生成的方法存根

	for (UserInfo value : values) {// 没有combiner的情况才，一个key有多少value就循环多少次，
	    // logger.fatal(" value.getGroups()：" + value.getGroups());
	    if (sum == 0) {// 此key的所有值中的第一个value
		userInfo = new UserInfo(value);
		// userInfo = new UserInfo(value);
	    } else {// 第一个key之外的其他所有value
		userInfo.addGroupList(value.getGroups());
	    }
	    sum++;
	}
	if (sum > 5) {
	    logger.fatal(sum + "  大于5个群的QQ号：" + key);
	}
	userInfo.setGroupCount(userInfo.getGroups().size());
	context.write(key, userInfo);
    }

    void count() {

    }

}
