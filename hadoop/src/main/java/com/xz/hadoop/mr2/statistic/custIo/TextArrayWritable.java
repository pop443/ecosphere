package com.xz.hadoop.mr2.statistic.custIo;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * 在MR中使用ArrayWritable 需要实例化具体实现对象
 * 本例实现ArrayWritable[Text]类型
 * 必须提供无参构造 不然reduce通过反射构造 Iterable<TextArrayWritable> 会出错
 * @author xz
 *
 */
public class TextArrayWritable extends ArrayWritable{
	
	public TextArrayWritable(){
		super(Text.class);
	}

	public TextArrayWritable(Class<? extends Writable> valueClass) {
		super(Text.class);
	}

	public TextArrayWritable(Class<? extends Writable> valueClass,
			Writable[] values) {
		super(Text.class, values);
	}

	public TextArrayWritable(String[] strings) {
		super(Text.class);
		Writable[] values = new Writable[strings.length] ;
		for (int i = 0; i < values.length; i++) {
			values[i] = new Text(strings[i]) ;
		}
		set(values);
	}

	

}
