package com.xz.flume.spooldir.source.file;

import java.io.IOException;
import java.util.*;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class FileReaderUtil {
	
	public static List<Event> readFiles(FileCenter center) throws IOException{
		long timestamp = System.currentTimeMillis() ;
		System.out.println("---------readFiles----------");
		Map<FileInfo, String> map = center.readLine() ;
		List<Event> list = new ArrayList<>() ;
		for (Map.Entry<FileInfo, String> entry : map.entrySet()) {
			String line = entry.getValue() ;
			System.out.println(entry.getKey().getAbsolutePath()+"--"+line);
			if (line == null) {
				continue ;
			}
			FileInfo fileInfo = entry.getKey() ;
			Event event = EventBuilder.withBody(line.getBytes("utf-8")) ;
			Map<String, String> eventMap = new HashMap<>() ;
			eventMap.put("timestamp",timestamp+"") ;
			eventMap.put("path",fileInfo.getAbsolutePath()) ;
			event.setHeaders(eventMap);
			list.add(event) ;
		}
		return list ;
	}
}
