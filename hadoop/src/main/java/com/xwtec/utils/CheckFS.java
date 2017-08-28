package com.xwtec.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Administrator on 2017-8-28.
 * like hdfs fsck / -
 */
public class CheckFS extends TimerTask{
    private FileSystem fs ;
    private Path path ;
    private URLConnectionFactory factory ;
    private Configuration conf ;
    public CheckFS(Configuration conf){
        try {
            this.conf = conf ;
            this.fs = FileSystem.get(conf) ;
            this.path = new Path("/user/root/.Trash/170828140000/icodeV3/c_login_server/20170828");
            this.factory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void run() {
        try {
            StringBuilder sb = new StringBuilder() ;
            sb.append("http://")
            .append(conf.get("dfs.namenode.http-address"))
            .append("/fsck?ugi=root&openforwrite=1&path=")
            .append(URLEncoder.encode(Path.getPathWithoutSchemeAndAuthority(fs.resolvePath(path)).toString(), "UTF-8"));
            System.out.println(sb.toString());
            URL path = new URL(sb.toString());
            URLConnection connection = factory.openConnection(path,true);

            InputStream stream = connection.getInputStream();
            BufferedReader input = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String line = null;
            StringBuilder lastLine = new StringBuilder();
            byte errCode = -1;

            try {
                while((line = input.readLine()) != null) {
                    lastLine.append(line).append("\r\n");
                }
            } finally {
                input.close();
            }
            System.out.println(lastLine);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AuthenticationException e) {
            e.printStackTrace();
        }


    }
    static class TmpPathFilter implements PathFilter{
        @Override
        public boolean accept(Path path) {
            return !path.getName().endsWith(".tmp");
        }
    }

    public static void main(String[] args) {
            Configuration conf = new Configuration();
            CheckFS checkFS = new CheckFS(conf) ;
            Timer timer = new Timer("checkFs");
            timer.scheduleAtFixedRate(checkFS,0,20000);

    }
}
