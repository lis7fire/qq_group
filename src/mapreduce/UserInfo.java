package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

public class UserInfo implements Writable {
    private String qqNUM;
    private String age;
    private String sex;
    private int groupCount;

    // private userGroup groupObj = new userGroup();// 可以删除，没有用了。
    private List<userGroup> groups = new LinkedList<>();

    // List<Writable> li = new LinkedList<Writable>();
    // ListWritable cleanl = new ListWritable(li);

    public UserInfo() {
	// TODO 自动生成的构造函数存根
    }

    public UserInfo(String qqNUM, String nick, String age, String sex,
	    String position, String groupNum) {
	this(qqNUM, age, sex);
	// this.groupObj = new userGroup(nick, position, groupNum);

	this.groups.add(new userGroup(nick, position, groupNum));
    }

    UserInfo(String qqNUM, String age, String sex) {
	this.qqNUM = qqNUM;
	this.age = age;
	this.sex = sex;
	this.groupCount = 0;
    }

    UserInfo(String qqNUM, String age, String sex, userGroup u) {
	this(qqNUM, age, sex);
	// this.groupObj = new userGroup(u);
	this.groups.add(new userGroup(u));
    }

    UserInfo(String qqNUM, String age, String sex, List<userGroup> groups) {// 废弃掉
	this(qqNUM, age, sex);
	for (userGroup userGroup : groups) {
	    this.groups.add(new userGroup(userGroup));
	}
	// System.out.println("userinfo构造器：" + this.groups);
    }

    public UserInfo(UserInfo value) {
	// TODO 自动生成的构造函数存根
	this(value.qqNUM, value.age, value.sex);
	for (userGroup userGroup : value.getGroups()) {
	    this.groups.add(new userGroup(userGroup));
	}
	// System.out.println("userinfo构造器：" + this.groups);
    }

    public void addGroup(userGroup addgroup) {
	// 增加本qq号的群号信息
	// this.li.add(addgroup);

	this.groups.add(new userGroup(addgroup));
	// System.out.println("addGroup：" + this.groups);
	// this.toJson = gson.toJson(groups);
    }

    // 所有java中 对象的赋值/初始化(也就是使用等号建立连接)传递的都是指针，而不是复制对象空间，这是浅拷贝
    // 只有基本类型的赋值(也是使用等号)的深拷贝，复制具体堆值而不是指针。
    public void addGroupList(List<userGroup> groupList) {
	// 增加本qq号的群号信息
	this.groups.addAll(groupList); // 这里的addall是浅拷贝，
	// 只是在groups的栈中新建一个节点Node保存了指向将groupList的指针,如果下面立即修改groupList里面已有节点的值(这个修改是在堆中)，groups里面的值也会改变。
	// 但是在groupList中增加/删除节点，groups的内容不变。
	// groupList.get(0).setNick("执行这句会同时修改groups中的相应的值，因为他们两个指针指向同一个堆空间");

	// System.out.println("addGroup：" + this.groups);
    }

    @SuppressWarnings("null")
    @Override
    public void readFields(DataInput in) throws IOException {
	// 自定义反序列化方法

	this.qqNUM = in.readUTF();
	this.age = in.readUTF();
	this.sex = in.readUTF();
	// this.groupCount = in.readInt();
	// this.groupObj.readFields(in);

	int size = in.readInt(); // construct values
	// value = this.groupObj;
	// System.out.println(size + " 反序列化：" + value);
	// this.groups = new LinkedList<>();
	this.groups.clear();

	for (int i = 0; i < size; i++) {
	    userGroup value = new userGroup();
	    value.readFields(in); // read a value
	    // groups.add(value); // store it in values
	    this.groups.add(value); // store it in values
	}
	// System.out.println(size + " 反序列化：" + groups);

    }

    @Override
    public void write(DataOutput out) throws IOException {
	// 自定义序列化函数
	out.writeUTF(qqNUM);
	out.writeUTF(age);
	out.writeUTF(sex);
	// out.writeInt(groupCount);
	// this.groupObj.write(out);

	// 以下是针对List类型的groups序列化
	// out.writeUTF(groups.getClass().getName());
	// out.writeUTF(userGroup.class.getName());

	out.writeInt(groups.size()); // write values
	Iterator<userGroup> iterator = groups.iterator();

	while (iterator.hasNext()) {
	    // System.out.println("序列化 groups ：" + groups.get(0));
	    iterator.next().write(out);
	}

    }

    @Override
    public String toString() {
	// TODO 自动生成的方法存根
	Gson json = new Gson();
	String jsonObj = json.toJson(this);
	return jsonObj;
    }

    @Override
    public int hashCode() {
	// TODO 自动生成的方法存根
	return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
	// TODO 自动生成的方法存根
	return super.equals(obj);
    }

    public String getQqNUM() {
	return qqNUM;
    }

    public void setQqNUM(String qqNUM) {
	this.qqNUM = qqNUM;
    }

    public List<userGroup> getGroups() {
	return groups;
    }

    public void setGroups(List<userGroup> groups) {
	this.groups = groups;
    }

    public String getAge() {
	return age;
    }

    public void setAge(String age) {
	this.age = age;
    }

    public int getGroupCount() {
	return groupCount;
    }

    public void setGroupCount(int groupCount) {
	this.groupCount = groupCount;
    }

    public String getSex() {
	return sex;
    }

    public void setSex(String sex) {
	this.sex = sex;
    }

    /**
     * List集合转为ListWritable的方法
     */
    public static ListWritable cleanListWritable(List cleanRuleList) {

	List<Writable> li = new LinkedList<Writable>();
	ListWritable cleanl;
	if (!cleanRuleList.isEmpty()) {
	    try {
		for (int i = 0; i < cleanRuleList.size(); i++) {
		    userGroup aGroup = (userGroup) cleanRuleList.get(i);
		    li.add(aGroup);
		}
		cleanl = new ListWritable(li);
		return cleanl;
	    } catch (Exception e) {
		System.err.println("发生异常" + e);
	    }
	}
	return null;
    }

    class userGroup implements Writable, Cloneable {
	private String groupNum;
	private String nick;
	private String position;

	public userGroup() {
	}

	public userGroup(userGroup ug) {
	    // TODO 自动生成的构造函数存根
	    this(ug.getNick(), ug.getPosition(), ug.getGroupNum());
	}

	public userGroup(String nick2, String position2, String groupNum2) {
	    // TODO 自动生成的构造函数存根
	    this.groupNum = groupNum2;
	    this.nick = nick2;
	    this.position = position2;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	    // TODO 自动生成的方法存根
	    this.groupNum = arg0.readUTF();
	    this.nick = arg0.readUTF();
	    this.position = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	    // TODO 自动生成的方法存根
	    arg0.writeUTF(groupNum);
	    arg0.writeUTF(nick);
	    arg0.writeUTF(position);
	}

	@Override
	protected Object clone() { // 实现深拷贝的方法
	    // TODO 自动生成的方法存根
	    userGroup uGroup = null;
	    try {
		uGroup = (userGroup) super.clone();
		uGroup = new userGroup(this.nick, this.position, this.groupNum);
	    } catch (CloneNotSupportedException e) {
		// TODO 自动生成的 catch 块
		e.printStackTrace();
	    }
	    return uGroup;
	}

	public String getGroupNum() {
	    return groupNum;
	}

	public void setGroupNum(String groupNum) {
	    this.groupNum = groupNum;
	}

	public String getNick() {
	    return nick;
	}

	public void setNick(String nick) {
	    this.nick = nick;
	}

	public String getPosition() {
	    return position;
	}

	public void setPosition(String position) {
	    this.position = position;
	}

    }

}
