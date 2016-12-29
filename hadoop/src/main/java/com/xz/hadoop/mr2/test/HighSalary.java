package com.xz.hadoop.mr2.test;

import java.io.IOException;

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
 * 工资前三名的员工姓名及工资
 */

@SuppressWarnings("deprecation")
public class HighSalary extends Configured implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String[] kv;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			kv = line.toString().split(",");
			context.write(new Text("1"), new Text(kv[1].trim() + "," + kv[5].trim()));
		}
	}

	static class MyReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int firstHigh = 0;
			int secondHigh = 0;
			int thirdHigh = 0;
			String firstHighName = "";
			String secondHighName = "";
			String thirdHighName = "";
			for (Text value : values) {
				System.out.println("value:"+value.toString());
				String[] info = value.toString().split(",");
                int salary = Integer.parseInt(info[1]);
                if(firstHigh<salary){
                	thirdHigh = secondHigh;
                	thirdHighName = secondHighName;
                	secondHigh = firstHigh;
                	secondHighName = firstHighName;
                	firstHigh = salary;
                	firstHighName = info[0];
                }else if(secondHigh<salary){
                	thirdHigh = secondHigh;
                	thirdHighName = secondHighName;
                	secondHigh = salary;
                	secondHighName = info[0];
                }else if(thirdHigh<salary){
                	thirdHigh = salary;
                	thirdHighName = info[0];
                }
			}
			context.write(new Text(firstHighName), new Text(String.valueOf(firstHigh)));
			context.write(new Text(secondHighName), new Text(String.valueOf(secondHigh)));
			context.write(new Text(thirdHighName), new Text(String.valueOf(thirdHigh)));
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
		int res = ToolRunner.run(new Configuration(), new HighSalary(), args);
		System.exit(res);
	}
}
