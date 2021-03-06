package qun_main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import mapreduce.UserInfo;

public class Qun {
    static Logger logger = LogManager.getLogger(Qun.class);

    static boolean temp() {
	boolean res = false;
	for (int i = 0; i < 11; i++) {
	    File outfile = new File("E:/qqData/GroupDatas/GroupData" + i);
	    if (!outfile.exists()) {
		res = outfile.mkdirs();
	    }
	}
	return res;
    }

    public static void main(String[] args) {
	// TODO Auto-generated method stub
	logger.info("这里是qq群关系的主程序：");
	logger.warn("文件夹创建 ：" + temp());
	logger.trace("文件夹创建 ：");
	HashMap<Integer, ArrayList<Integer>> range = new HashMap<>();
	for (int i = 1; i < 11; i++) {
	    ArrayList<Integer> tmp = new ArrayList<>();
	    tmp.add((i - 1) * 100 + 1);
	    tmp.add(i * 100);
	    range.put(i, tmp);
	}
	logger.trace(range);
	logger.trace(range.get(1));
	logger.trace(range.get(1).get(0));
	logger.trace(range.get(1).get(1));
	Gson temp = new Gson();
	UserInfo userInfo = new UserInfo("3935212", "haha", "18", "1", "5",
		"182728");

	Map<String, String> mapGroup = new HashMap<>();
	mapGroup.put("groupNum", "qw111");
	mapGroup.put("nick", "赵日天");
	mapGroup.put("position", "2");
	// userInfo.addGroup(mapGroup);

	String jsonObj = temp.toJson(userInfo);
	logger.info(jsonObj);
	logger.info(temp.toJson("111111"));
	logger.info(temp.fromJson("\"99.99\"", double.class));

    }

}
