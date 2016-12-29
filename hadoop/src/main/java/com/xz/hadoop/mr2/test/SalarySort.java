package com.xz.hadoop.mr2.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * 根据员工工资+提成排序输出
 */
public class SalarySort extends Configured implements Tool {
	 public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {
		 
	        public void map(LongWritable key, Text value, Context context) throws IOException,     InterruptedException {
	 
	            // 对员工文件字段进行拆分
	            String[] kv = value.toString().split(",");
	 
	            // 输出key为员工所有工资和value为员工姓名
	            int empAllSalary = "".equals(kv[6]) ? Integer.parseInt(kv[5]) : Integer.parseInt(kv[5]) + Integer.parseInt(kv[6]);
	            context.write(new IntWritable(empAllSalary), new Text(kv[1]));
	            System.out.println("map结束");
	        }
	    }
	 
	    /**
	     * 递减排序算法
	     */
	    public static class DecreaseComparator extends Comparator {
	 
	        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        	System.out.println("排序开始");
	            return -super.compare(b1, s1, l1, b2, s2, l2);
	        }
	    }
	 
	    @Override
	    public int run(String[] args) throws Exception {
	 
	        // 实例化作业对象，设置作业名称
	        Job job = new Job(getConf(), "Q9EmpSalarySort");
	        job.setJobName("Q9EmpSalarySort");
	 
	        // 设置Mapper和Reduce类
	        //job.setJarByClass(SalarySort.class);
	        job.setMapperClass(MapClass.class);
	 
	        // 设置输出格式类
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setSortComparatorClass(DecreaseComparator.class);
	 
	        // 第1个参数为员工数据路径和第2个参数为输出路径
	        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),                   args).getRemainingArgs();
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	 
	        job.waitForCompletion(true);
	        return job.isSuccessful() ? 0 : 1;
	    }
	/**
	 * 主方法，执行入口
	 * 
	 * @param args
	 *            输入参数
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SalarySort(), args);
		System.exit(res);
	}
	

}
