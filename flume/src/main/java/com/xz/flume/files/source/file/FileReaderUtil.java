package com.xz.flume.files.source.file;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class FileReaderUtil {
    private static Lock lock = new ReentrantLock();
    private static Map<FileInfo, String> map = new HashMap<>();

    public static List<Event> readFiles(FileCenter center, int batchNu) throws IOException {
        List<Event> list = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        System.out.println("---------readFiles----------");
        lock.lock();
        try {
            map.clear();
            center.readLine(map, batchNu);

            for (Map.Entry<FileInfo, String> entry : map.entrySet()) {
                String line = entry.getValue();
                System.out.println(entry.getKey().getAbsolutePath() + "--" + line);
                if (line == null) {
                    continue;
                }
                FileInfo fileInfo = entry.getKey();
                Event event = EventBuilder.withBody(line.getBytes("utf-8"));
                Map<String, String> eventMap = new HashMap<>();
                eventMap.put("timestamp", timestamp + "");
                eventMap.put("path", fileInfo.getAbsolutePath());
                event.setHeaders(eventMap);
                list.add(event);
            }
        } finally {
            lock.unlock();
        }
        return list;
    }
}
