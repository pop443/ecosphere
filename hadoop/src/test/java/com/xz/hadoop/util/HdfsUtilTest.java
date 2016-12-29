package com.xz.hadoop.util;

import com.xz.hadoop.conf.HadoopConf;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by xz on 2016/10/10.
 */
public class HdfsUtilTest {
    private HdfsUtil hdfsUtil;

    @Before
    public void init() {
        hdfsUtil = HdfsUtil.getInstance();
    }

    @Test
    public void testShowFile(){
        Path path = new Path(HadoopConf.getHdfsPrefixpath()+"/xz/hivedemo");
        String s = hdfsUtil.showFile(path) ;
        System.out.println(s);
    }

}
