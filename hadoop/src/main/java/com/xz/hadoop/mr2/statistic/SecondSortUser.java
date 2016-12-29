package com.xz.hadoop.mr2.statistic;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.statistic.custIo.TextArrayWritable;
import com.xz.hadoop.mr2.statistic.custIo.User;
import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;
/**
 * 二次排序 按照用户的deptno与age排序 
 * 使用user序列化对象 
 * 内包含的字段同时为序列化对象
 * @author root
 *
 */
public class SecondSortUser extends Configured implements Tool {

	public static class SecondSortUserMapper extends
			Mapper<LongWritable, Text, User, TextArrayWritable> {
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
			String userid = strs[0];
			String age =  strs[1];
			String name = strs[2];
			String deptno = strs[3];
			User user = new User(new IntWritable(Integer.parseInt(deptno)), new IntWritable(Integer.parseInt(age))) ;
			TextArrayWritable arrayWritable = new TextArrayWritable(new String[]{userid,name}) ;
			context.write(user, arrayWritable);
		}

	}

	public static class SecondSortUserReduce extends
			Reducer<User, TextArrayWritable, Text, Text> {

		@Override
		protected void reduce(User key, Iterable<TextArrayWritable> values,
				Context context)
				throws IOException, InterruptedException {
			for (TextArrayWritable textArrayWritable : values) {
				context.write(new Text(key.getDeptno()+","+key.getAge()), new Text(textArrayWritable.toStrings()[0]+","+textArrayWritable.toStrings()[1]));
			}
		}
	}
	
	public static class SecondSortUserPartition extends Partitioner<User, TextArrayWritable>{
		@Override
		public int getPartition(User user, TextArrayWritable textArrayWritable, int numPartition) {
			return Math.abs(user.getDeptno().get()*127)%numPartition;
		}
	}
	
	public static class SecondSortUserComparator extends WritableComparator{
		public SecondSortUserComparator(){
			super(User.class,true) ;
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			User u1 = (User)a ;
			User u2 = (User)b ;
			int deptno1 = u1.getDeptno().get() ;
			int deptno2 = u2.getDeptno().get() ;
			return deptno1==deptno2?0:(deptno1<deptno2?-1:1);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf());

		job.setJobName("SecondSortUser");

		job.setJarByClass(SecondSortUser.class);
		job.setMapperClass(SecondSortUserMapper.class);
		job.setReducerClass(SecondSortUserReduce.class);
		job.setPartitionerClass(SecondSortUserPartition.class);
		job.setGroupingComparatorClass(SecondSortUserComparator.class);
		job.setNumReduceTasks(2);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);
		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(User.class);
		job.setOutputValueClass(TextArrayWritable.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径

		
		// FileSystem fileSystem = FileSystem.get(getConf()) ;
		FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		FileInputFormat.setInputPaths(job, new Path(arg0[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String inPath1 = "/user";
		String outPath = "/result";
		MRPaths args2 = new MRPaths(outPath,inPath1) ;
		String[] arg = args2.getArgsPaths() ;
		int ret = ToolRunner.run(MRUtil.getConfiguration(null),
				new SecondSortUser(), arg);
		System.exit(ret);
	}

}
