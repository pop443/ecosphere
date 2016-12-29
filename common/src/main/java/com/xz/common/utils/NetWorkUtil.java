package com.xz.common.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * falcon -- 2016/11/24.
 */
public class NetWorkUtil {

    public static String[] getLocalIpList(){
        List<String> list = new ArrayList<>() ;
        try {
            Enumeration<NetworkInterface> enumerations = NetworkInterface.getNetworkInterfaces() ;
            while (enumerations.hasMoreElements()) {
                NetworkInterface netInterface = enumerations.nextElement();
                // 每个网络地址都有多个网络地址 回环地址（lookback） sitelocal IPV4 IPV6
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                String back = null ;
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if(address instanceof Inet6Address){
                        continue;
                    }
                    if (address.isSiteLocalAddress() && !address.isLoopbackAddress()) {
                        list.add(address.getHostAddress());
                        continue;
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return list.toArray(new String[list.size()]) ;
    }

    public static void main(String[] args) {
        String[] array = NetWorkUtil.getLocalIpList();
        System.out.println(array[0]);
    }
}
