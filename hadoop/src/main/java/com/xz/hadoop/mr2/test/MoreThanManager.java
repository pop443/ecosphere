package com.xz.hadoop.mr2.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 题目:找出比上司工资搞的员工姓名与工资
 */
@SuppressWarnings("deprecation")
public class MoreThanManager extends Configured implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String[] kv;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			kv = line.toString().split(",");
			// 员工表数据
			context.write(new Text(kv[0]), new Text("M," + kv[5]));
			// 员工对应的经理表数据
			context.write(new Text(kv[3]), new Text("E," + kv[1] + "," + kv[5]));
		}
	}

	static class MyReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			 // 定义员工姓名、工资和存放部门员工Map
            String empName;
            long empSalary = 0;
            HashMap<String, Long> empMap = new HashMap<String, Long>();
           
            // 定义经理工资变量
            long mgrSalary = 0;
 
            for (Text val : values) {
            	System.out.println(val);
                if (val.toString().startsWith("E")) {
                    // 当是员工标示时，获取该员工对应的姓名和工资并放入Map中
                    empName = val.toString().split(",")[1];
                    empSalary = Long.parseLong(val.toString().split(",")[2]);
                    empMap.put(empName, empSalary);
                } else {
                    // 当时经理标志时，获取该经理工资
                    mgrSalary = Long.parseLong(val.toString().split(",")[1]);
                }
            }
 
			// 遍历该经理下属，比较员工与经理工资高低，输出工资高于经理的员工
			for (Entry<String, Long> entry : empMap.entrySet()) {
				if (entry.getValue() > mgrSalary) {
					context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "Q3EarliestEmp");
		job.setJobName("Q3EarliestEmp");
		// job.setJarByClass(EarliestEmp.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

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
		int res = ToolRunner.run(new Configuration(), new MoreThanManager(), args);
		System.exit(res);
	}
}
