package com.xz.hadoop.mr2.init;

import com.xz.hadoop.conf.HadoopConf;
import com.xz.hadoop.util.HdfsUtil;
import org.apache.hadoop.fs.Path;

public class InitDate {
	public static final String dstUserPathStr = "/user" ;
	public static final String dstDeptPathStr = "/dept" ;
	public static final String srcUserPathStr = "hadoop/user.txt" ;
	public static final String srcDeptPathStr = "hadoop/dept.txt" ;
	
	public static void main(String[] args) {

		boolean dstUserBo = false ;
		boolean dstDeptBo = false ;
		HdfsUtil hdfsUtil = HdfsUtil.getInstance() ;
		Path dstUserPath = new Path(HadoopConf.getMr2DemoPrefixpath()+InitDate.dstUserPathStr) ;
		Path dstDeptPath = new Path(HadoopConf.getMr2DemoPrefixpath()+InitDate.dstDeptPathStr) ;
		hdfsUtil.removePath(dstUserPath) ;
		hdfsUtil.removePath(dstDeptPath) ;
		dstUserBo = hdfsUtil.mkdir(dstUserPath) ;
		dstDeptBo = hdfsUtil.mkdir(dstDeptPath) ;
		System.out.println(dstUserBo);
		if (dstUserBo && dstDeptBo) {
			String path = InitDate.class.getClassLoader().getResource("").getPath() ;
			
			String srcUserPathStr = path+InitDate.srcUserPathStr ;
			String srcDeptPathStr = path+InitDate.srcDeptPathStr ;
			
			Path srcUserPath = new Path(srcUserPathStr) ;
			Path srcDeptPath = new Path(srcDeptPathStr) ;

			hdfsUtil.put(srcUserPath,dstUserPath) ;
			hdfsUtil.put(srcDeptPath,dstDeptPath) ;
		}
		
	}
}
