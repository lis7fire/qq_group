package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapreduce.UserInfo.userGroup;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QunMapper extends Mapper<LongWritable, Text, Text, UserInfo> {
    static Logger logger = LogManager.getLogger(QunMapper.class);
    static int nullRow = 0;
    static long allRow = 0;// 统计总共读取了多少非空行，在MR的结果中 Map input records 包含了空行
    static IntWritable ONE = new IntWritable(1);
    static Text qqNum;
    // 初始化avro的模式：
    private static final String NAME = "{"
	    + "	\"namespace\": \"example.avro\"," + "	\"type\": \"record\","
	    + "	\"name\": \"UserInfoAvro\"," + "	\"fields\": ["
	    + "		{\"name\": \"qqNum\",\"type\": \"string\"},"
	    + "		{\"name\": \"age\",\"type\": \"int\"},"
	    + "		{\"name\": \"sex\",\"type\": \"int\"},"
	    + "		{\"name\": \"groups\",\"type\": {"
	    + "				\"type\": \"array\"," + "				\"items\": {"
	    + "					\"type\": \"record\",\"name\": \"Group\","
	    + "					\"fields\": ["
	    + "						{\"name\": \"groupNum\",\"type\": \"string\"},"
	    + "						{\"name\": \"nick\",\"type\": \"string\"},"
	    + "						{\"name\": \"diwei\",\"type\": \"int\"}" + "					]"
	    + "				}" + "			}" + "		}" + "	]" + "}";
    private static final String USERSCHEMA = "{"
	    + "   \"namespace\": \"example.avro\","
	    + "   \"type\": \"record\","
	    + "   \"name\": \"UserInfoAvro\","
	    + "   \"fields\": ["
	    + "       {\"name\": \"qqNum\",\"type\": \"string\"},"
	    + "       {\"name\": \"age\",\"type\": \"int\"},"
	    + "       {\"name\": \"sex\",\"type\": \"int\"},"
	    + "       {\"name\": \"groups\",\"type\": {\"type\": \"array\",\"items\": \"Group\"}"
	    + "       }" + "   ]" + "}";
    private static final String GROUPSCHEMA = "{"
	    + "   \"namespace\": \"example.avro\","
	    + "   \"type\": \"record\"," + "   \"name\": \"Group\","
	    + "   \"fields\": ["
	    + "       {\"name\": \"groupNum\",\"type\": \"string\"},"
	    + "       {\"name\": \"nick\",\"type\": \"string\"},  "
	    + "       {\"name\": \"diwei\",\"type\": \"int\"}" + "   ]" + "}";

    static Schema schema = new Schema.Parser().parse(NAME);// 可以删除
    static Schema schemaGroup = new Schema.Parser().parse(GROUPSCHEMA);// 可以删除
    GenericRecord aRecord = new GenericData.Record(schema);// 可以删除
    GenericRecord groupRecord = new GenericData.Record(schemaGroup);// 可以删除
    UserInfo userInfo = new UserInfo();
    userGroup group = userInfo.new userGroup();// 可以删除
    Map<String, String> aGroup = new HashMap<>();// 可以删除
    List<userGroup> tGroups;// 可以删除

    public QunMapper() {
	// TODO 自动生成的构造函数存根
	// 从文件中加载avro的策略，也可以直接写string来现场定义
	// schema = new Schema.Parser().parse( //上面用了string定义，这边就不需要了
	// getClass().getResourceAsStream("./conf/avroFormat.avsc"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	// TODO 自动生成的方法存根
	String line = value.toString();
	String[] cells = line.split(";");// 将每一行分割；
	if (cells[0] == null || cells[0].isEmpty()) {
	    nullRow++;
	    logger.info("这是第几空行:" + nullRow);// 空行自动忽略，不向reducer发送
	} else {// 非空行
	    qqNum = new Text(cells[0]);
	    allRow++;
	    // parseToAvro(cells);

	    // printQQ(cells[0], line);
	    // parseToObj(cells);
	    UserInfo mapOut = new UserInfo(cells[0], cells[1], cells[2],
		    cells[3], cells[4], cells[5]);
	    // context.write(qqNum, new AvroValue<GenericRecord>(aRecord));
	    context.write(qqNum, mapOut);
	    // logger.debug("userInfo: " + mapOut);
	}
    }

    void parseToObj(String[] cells) {
	// userGroup tmp = userInfo.new userGroup(cells[1], cells[4], cells[5]);
	// userInfo.setGroupObj(tmp);
	// tGroups.clear();
	// userInfo = new UserInfo(cells[0], cells[2], cells[3], tmp);
	// userInfo = new UserInfo(cells[0], cells[2], cells[3], tGroups);

	// userInfo.setQqNUM(cells[0]);
	// userInfo.setAge(cells[2]);
	// userInfo.setSex(cells[3]);
	//
	// group.setGroupNum(cells[5]);
	// group.setNick(cells[1]);
	// group.setPosition(cells[4]);
	// userInfo.setGroupObj(group);

	// aGroup.put("groupNum", cells[5]);
	// aGroup.put("nick", cells[1]);
	// aGroup.put("position", cells[4]);

	// userInfo.addGroup(aGroup);

	// Gson jsonObj = new Gson();
	// String gsonObj = jsonObj.toJson(userInfo);
	// return gsonObj;
    }

    GenericRecord parseToAvro(String[] cells) {
	// GenericArray<GenericRecord> groups=new GenericData.Array<>(1,schema);
	// aRecord.put("qqNum", cells[0]);
	List<GenericRecord> tmp = new ArrayList<GenericRecord>();
	tmp.add(groupRecord);
	groupRecord.put("groupNum", cells[5]);
	groupRecord.put("nick", cells[1]);
	groupRecord.put("diwei", cells[4]);
	aRecord.put("groups", tmp.get(0));
	aRecord.put("age", cells[2]);
	aRecord.put("sex", cells[3]);
	return aRecord;
    }

    void printQQ(String qqNum, String line) {
	if (qqNum.equals("476513197") || qqNum.equals("333888")
		|| qqNum.equals("20050606")) {
	    logger.debug("100721152的信息:" + line);
	}
    }

}
