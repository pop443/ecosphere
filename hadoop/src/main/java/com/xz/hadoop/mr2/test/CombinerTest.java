package com.xz.hadoop.mr2.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombinerTest extends Configured implements Tool {
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Before Mapper: " + key + ", " + value);
			String line = value.toString();
			context.write(new Text(line), one);
			System.out.println("After Mapper: =======" + new Text(line) + ", " + one);
		}
	}
    
	static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuffer sb = new StringBuffer();
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				sb.append(sum);
			}
			System.out.print("Before combiner: " + key + ", " + sb.toString());
			context.write(new Text(key), new IntWritable(sum));
			System.out.println(

					"======" +

			"After combiner: " + key + ", " + sum);
		}
	}
	
	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				System.out.print("Before Reduce: " + key + ", " + val.get());
				context.write(key, new IntWritable(val.get()));
				System.out.println(

						"======" +

				"After Reduce: " + key + ", " + val.get());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
        //Configuration conf = new Configuration();
        //conf.set("min.num.spill.for.combine", "0");
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "CombinerTest");
		job.setJobName("CombinerTest");
		// job.setJarByClass(EarliestEmp.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
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
		int res = ToolRunner.run(new Configuration(), new CombinerTest(), args);
		System.exit(res);
	}

}
