package com.xz.jstorm.demo.wordcount1;

import java.io.*;
import java.util.*;

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
    private List<LineNumberReader> readers = null ;
    private int index = 0 ;

    @Override
    public void nextTuple() {
        if (index==readers.size()){
            index = 0 ;
        }
        try {
            LineNumberReader lnr = readers.get(index) ;
            String line = lnr.readLine() ;
            System.out.println(Thread.currentThread().getName()+line);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(new Values(line)) ;
            index++ ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //初始化
        this.collector = collector ;
        File parentFile = new File(Constant.IN) ;
        IOFileFilter ioFileFilter = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")) ;
        List<File> files = (List<File>)FileUtils.listFiles(parentFile, ioFileFilter, null) ;
        readers = new ArrayList<>() ;
        try {
            for (File file:files) {
                FileReader fr = new FileReader(file);
                LineNumberReader lnr = new LineNumberReader(fr) ;
                readers.add(lnr);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}