package com.xz.flume.files.source.file;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class FileReaderUtil {
    private static Lock lock = new ReentrantLock();
    private static List<String> content = new ArrayList<>();

    public static List<Event> readFiles(FileCenter center, int batchNu) throws IOException {
        List<Event> list = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        System.out.println("---------readFiles----------");
        lock.lock();
        try {
            content.clear();
            center.readLine(content, batchNu);
            for (String line:content){
                if (line == null) {
                    continue;
                }
                Event event = EventBuilder.withBody(line.getBytes("utf-8"));
                Map<String, String> eventMap = new HashMap<>();
                eventMap.put("timestamp", timestamp + "");
                event.setHeaders(eventMap);
                list.add(event);
            }
        } finally {
            lock.unlock();
        }
        return list;
    }
}
