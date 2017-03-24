package com.xz.kafka.mbean;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * falcon -- 2017/3/23.
 */
public class Manager {
    public static void main(String[] args) {
        try {
            JMXServiceURL url = new JMXServiceURL(
                    "service:jmx:rmi:///jndi/rmi://172.32.149.129:10000/jmxrmi");
            Map<String,Object> map = new HashMap<>() ;
            map.put(JMXConnector.CREDENTIALS,new String[]{"root","root123."}) ;
            map.put("jmx.remote.x.request.waiting.timeout","3000") ;
            map.put("jmx.remote.x.notification.fetch.timeout","3000") ;
            map.put("sun.rmi.transport.connectionTimeout","3000") ;
            map.put("sun.rmi.transport.tcp.handshakeTimeout","3000") ;
            map.put("sun.rmi.transport.tcp.responseTimeout","3000") ;
            JMXConnector jmxc = JMXConnectorFactory.connect(url, map);
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // 把所有Domain都打印出来
            //printAllDomains(mbsc);
            getKafkaServerInfo(mbsc) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getKafkaServerInfo(MBeanServerConnection mbsc) {
        try {
            ObjectName objectName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec") ;
            MBeanInfo mbeanInfo = mbsc.getMBeanInfo(objectName) ;
            System.out.println(mbeanInfo);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (InstanceNotFoundException e) {
            e.printStackTrace();
        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (ReflectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printAllDomains(MBeanServerConnection mbsc){
        try {
            System.out.println("Domains:---------------");
            String domains[] = mbsc.getDomains();
            for (int i = 0; i < domains.length; i++)
            {
                System.out.println("\tDomain[" + i + "] = " + domains[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
