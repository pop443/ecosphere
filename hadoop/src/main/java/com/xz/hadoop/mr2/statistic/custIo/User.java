package com.xz.hadoop.mr2.statistic.custIo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
/**
 * 作为map阶段结束输出key 
 * 作为 序列化 比较对象
 * @author root
 *
 */
public class User  implements WritableComparable<User> {
	private IntWritable deptno ;
	private IntWritable age ;
	
	public User(){
		
	}
	
	public User(IntWritable deptno,IntWritable age){
		this.deptno = deptno ;
		this.age = age ;
	}
	
	public void set (IntWritable deptno,IntWritable age){
		this.deptno = deptno ;
		this.age = age ;
	}
	
	public IntWritable getDeptno() {
		return deptno;
	}

	public IntWritable getAge() {
		return age;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		Writable value = WritableFactories.newInstance(IntWritable.class);
		value.readFields(dataInput);
		this.deptno = (IntWritable)value ;
		
		value = WritableFactories.newInstance(IntWritable.class);
		value.readFields(dataInput);
		this.age = (IntWritable)value ;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.deptno.write(dataOutput);
		this.age.write(dataOutput);
	}

	@Override
	public int compareTo(User user) {
		int deptno1 = this.deptno.get() ;
		int deptno2 = user.getDeptno().get() ;
		int age1 = this.age.get() ;
		int age2 = user.getAge().get() ;
		if (deptno1!=deptno2) {
			return deptno1>deptno2?-1:1 ;
		}else if (age1!=age2) {
			return age1>age2?-1:1 ;
		}else{
			return 0 ;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o==null) {
			return false ;
		}
		if (this==o) {
			return true ;
		}
		if (o instanceof User) {
			User u = (User)o ;
			return u.getDeptno().get()==this.deptno.get() && u.getAge().get()==this.age.get() ;
		}else{
			return false ;
		}
	}

	@Override
	public int hashCode() {
		return this.deptno.get()*157+this.age.get();
	}
	

}
