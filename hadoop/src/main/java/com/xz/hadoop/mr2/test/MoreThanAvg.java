package com.xz.hadoop.mr2.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 题目:比平均工资要高的员工
 */

public class MoreThanAvg  extends Configured implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String[] kv;

		@Override
		//map输出为部门   员工姓名，工资
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=new String(value.getBytes(),0,value.getLength(),"GBK");
			kv = line.toString().split(",");
			context.write(new Text("Company"), new Text(kv[1]+","+kv[5]));
		}
	}

	static class MyReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String,String> map = new HashMap<>();
			int sum = 0;
			int num = 0;
			for (Text value : values) {
				String salary = value.toString().split(",")[1];
				sum += Integer.parseInt(salary);
				num++;
				map.put(value.toString().split(",")[0], value.toString().split(",")[1]);
			}
			int avg = sum/num; 
			context.write(new Text("the averge salary is :"), new Text(String.valueOf(avg)));
			context.write(new Text("fllowing is the answer"), new Text(""));
			Set<String> set = map.keySet();
			for (String key1 : set) {
				if(Integer.parseInt(map.get(key1))>avg){
					context.write(new Text(key1), new Text(map.get(key1)));
				}
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "Q3EarliestEmp");
		job.setJobName("Q3EarliestEmp");
		//job.setJarByClass(EarliestEmp.class);
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
		int res = ToolRunner.run(new Configuration(), new MoreThanAvg(), args);
		System.exit(res);
	}
}
