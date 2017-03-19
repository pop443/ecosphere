package com.xz.jstorm.demo.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import shade.storm.org.apache.commons.io.FileUtils;
import shade.storm.org.apache.commons.io.filefilter.FileFilterUtils;
import shade.storm.org.apache.commons.io.filefilter.IOFileFilter;

/**
 * falcon -- 2017/3/19.
 */
public class WordCountSpout extends BaseRichSpout{

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private static File parentFile ;

    @Override
    public void nextTuple() {
        try {
            IOFileFilter ioFileFilter = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")) ;
            Collection<File> files = FileUtils.listFiles(parentFile, ioFileFilter, null) ;
            Iterator<File> it = files.iterator() ;
            while (it.hasNext()) {
                File file = it.next() ;
                List<String> lines = FileUtils.readLines(file, "UTF-8");
                for (int i = 0; i < lines.size(); i++) {
                    collector.emit(new Values(lines.get(i))) ;
                }
                FileUtils.moveFile(file, new File(file.getPath() + System.currentTimeMillis() + ".bak"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //初始化
        this.collector = collector ;
        parentFile = new File(Constant.IN) ;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}