package com.xz.hadoop.mr2.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xz.hadoop.mr2.statistic.custIo.TextArrayWritable;
import com.xz.hadoop.mr2.util.MRPaths;
import com.xz.hadoop.mr2.util.MRUtil;

/**
 * MR方式 不走内存 select t1.userid,t1.name,t2.deptno,t2.deptname from user t1,dept t2 where t1.deptno=t2.deptno
 * 
 * @author root
 *
 */
public class QueryUserDeptName1 extends Configured implements Tool {

	private static String typeDept = "tag-dept:";
	private static String typeUser = "tag-user:";

	public static class QueryUserDeptNameMapper extends
			Mapper<LongWritable, Text, Text, TextArrayWritable> {
		private static Pattern pattern = Pattern.compile(",");
		private static Pattern pattern2 = Pattern.compile(":");

		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			// 首行不处理
			if (line.startsWith("tag:")) {
				return;
			}
			TextArrayWritable arrayWritable = null;
			// 区分两个文件
			if (line.startsWith(typeDept)) {
				// 处理dept文件
				String deptText = pattern2.split(line)[1];
				String[] deptInfo = pattern.split(deptText);
				arrayWritable = new TextArrayWritable(new String[] { typeDept,
						deptInfo[1] });
				System.out.println(deptInfo[1]);
				context.write(new Text(deptInfo[0]), arrayWritable);
			} else if (line.startsWith(typeUser)) {
				// 处理user文件
				String userText = pattern2.split(line)[1];
				String[] userInfo = pattern.split(userText);
				arrayWritable = new TextArrayWritable(new String[] { typeUser,
						userInfo[0], userInfo[1], userInfo[2] });
				System.out.println(userInfo[0]+"--"+userInfo[1]+"--"+userInfo[2]);
				context.write(new Text(userInfo[3]), arrayWritable);
			}

		}

	}

	public static class QueryUserDeptNameReduce extends
			Reducer<Text, TextArrayWritable, Text, Text> {

		@Override
		protected void reduce(Text inText, Iterable<TextArrayWritable> values,
				Context context)
				throws IOException, InterruptedException {
			String deptno = inText.toString();
			List<String[]> deptList = new ArrayList<>();
			List<String[]> userList = new ArrayList<>();
			for (TextArrayWritable arrayWritable : values) {
				String[] holeInfo = arrayWritable.toStrings();
				if (holeInfo[0].equals(typeDept)) {
					// 代表是dept的
					deptList.add(new String[]{holeInfo[1]}) ;
					System.out.println(holeInfo[1]);
				} else if (holeInfo[0].equals(typeUser)) {
					// 代表是user的
					userList.add(new String[]{holeInfo[1],holeInfo[2],holeInfo[3]}) ;
					System.out.println(holeInfo[1]+"--"+holeInfo[2]+"--"+holeInfo[3]);
				}
			}
			StringBuffer sb = null ;
			for (int i = 0; i < deptList.size(); i++) {
				for (int j = 0; j < userList.size(); j++) {
					sb = new StringBuffer() ;
					sb.append(deptList.get(i)[0]).append(",").append(userList.get(j)[0])
					.append(",").append(userList.get(j)[1])
					.append(",").append(userList.get(j)[2]);
					context.write(new Text(deptno), new Text(sb.toString()));
				}
			}

		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf());

		job.setJobName("QueryUserDeptName1");

		job.setJarByClass(QueryUserDeptName1.class);
		job.setMapperClass(QueryUserDeptNameMapper.class);
		job.setReducerClass(QueryUserDeptNameReduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径

		// FileSystem fileSystem = FileSystem.get(getConf()) ;
		FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		FileInputFormat.addInputPath(job, new Path(arg0[1]));
		FileInputFormat.addInputPath(job, new Path(arg0[2]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String outPath = "/result";
		String inPath1 = "/dept";
		String inPath2 = "/user";
		MRPaths args2 = new MRPaths(outPath, inPath1, inPath2);
		String[] arg = args2.getArgsPaths();
		int ret = ToolRunner.run(MRUtil.getConfiguration(null),
				new QueryUserDeptName1(), arg);
		System.exit(ret);
	}

}
