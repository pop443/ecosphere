package com.xz.hive.init;

import java.io.File;

import org.apache.hadoop.fs.Path;

import com.xz.hadoop.conf.HadoopConf;
import com.xz.hadoop.util.HdfsUtil;

public class InitDate {
	public static final String dstUserPathStr = "/user" ;
	public static final String dstDeptPathStr = "/dept" ;
	public static final String srcUserPathStr = "hive/user.txt" ;
	public static final String srcDeptPathStr = "hive/dept.txt" ;
	
	public static void main(String[] args) {
		boolean dstUserBo = false ;
		boolean dstDeptBo = false ;
		HdfsUtil hdfsOperation = HdfsUtil.getInstance() ;
		Path dstUserPath = new Path(HadoopConf.getHiveDemoPrefixpath()+InitDate.dstUserPathStr) ;
		Path dstDeptPath = new Path(HadoopConf.getHiveDemoPrefixpath()+InitDate.dstDeptPathStr) ;
		hdfsOperation.removePath(dstUserPath) ;
		hdfsOperation.removePath(dstDeptPath) ;
		dstUserBo = hdfsOperation.mkdir(dstUserPath) ;
		dstDeptBo = hdfsOperation.mkdir(dstDeptPath) ;
		System.out.println(dstUserBo);
		if (dstUserBo && dstDeptBo) {
			String path = InitDate.class.getClassLoader().getResource("").getPath() ;
			
			String srcUserPathStr = path+InitDate.srcUserPathStr ;
			String srcDeptPathStr = path+InitDate.srcDeptPathStr ;
			
			Path srcUserPath = new Path(srcUserPathStr) ;
			Path srcDeptPath = new Path(srcDeptPathStr) ;
			
			hdfsOperation.put(srcUserPath,dstUserPath) ;
			hdfsOperation.put(srcDeptPath,dstDeptPath) ;
		}
		
	}
}
