package com.xz.hadoop.mr2.util;

import org.apache.hadoop.fs.Path;

import com.xz.hadoop.conf.HadoopConf;
import com.xz.hadoop.util.HdfsUtil;

public class MRPaths {
	private String[] argsPaths;

	public MRPaths(String outPath,String... inPath) {
		argsPaths = new String[inPath.length+1] ;
		outPath = HadoopConf.getMr2DemoPrefixpath()+outPath ;
		argsPaths[0] = outPath;
		for (int i = 0; i < inPath.length; i++) {
			argsPaths[i+1] = HadoopConf.getMr2DemoPrefixpath()+inPath[i] ;
		}
		if (outPath != null ) {
			this.dealOutput(outPath);
		}
	}
	
	private void dealOutput(String outPath){
		HdfsUtil hdfsOperation = HdfsUtil.getInstance() ;
		Path path = new Path(outPath) ;
		hdfsOperation.removePath(path);
	}

	public String[] getArgsPaths() {
		return argsPaths;
	}
}
