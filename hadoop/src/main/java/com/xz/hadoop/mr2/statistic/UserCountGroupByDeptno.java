package com.xz.hadoop.mr2.statistic;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;

public class UserCountGroupByDeptno extends Configured implements Tool {

	 static class UserCountGroupByDeptnoMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static Pattern pattern = Pattern.compile(",");

		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString() ;
			
			if (line.startsWith("tag:")) {
				return ;
			}
			String[] strs = pattern.split(line);
			String deptno = strs[0];
			context.write(new Text(deptno), new IntWritable(1));
		}

	}

	 static class UserCountGroupByDeptnoReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(new Text(key), new IntWritable(sum));
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf());

		job.setJobName("UserCountGroupByDeptno");

		job.setJarByClass(UserCountGroupByDeptno.class);
		job.setMapperClass(UserCountGroupByDeptnoMapper.class);
		job.setReducerClass(UserCountGroupByDeptnoReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径

		
		// FileSystem fileSystem = FileSystem.get(getConf()) ;
		FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		FileInputFormat.setInputPaths(job, new Path(arg0[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String inPath1 = "/dept";
		String outPath = "/result";
		MRPaths args2 = new MRPaths(outPath,inPath1) ;
		String[] arg = args2.getArgsPaths() ;
		int ret = ToolRunner.run(MRUtil.getConfiguration(null),
				new UserCountGroupByDeptno(), arg);
		System.exit(ret);
	}

}
