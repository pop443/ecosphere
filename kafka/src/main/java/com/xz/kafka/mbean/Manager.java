package com.xz.kafka.mbean;

import com.yammer.metrics.reporting.JmxReporter;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * falcon -- 2017/3/23.
 */
public class Manager {
    public static void main(String[] args) {
        try {
            JMXServiceURL url = new JMXServiceURL(
                    "service:jmx:rmi:///jndi/rmi://172.32.148.245:9991/jmxrmi");
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
            printAllDomains(mbsc);
            getMinFetchRate(mbsc) ;
            getMessagesInPerSec(mbsc) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getMinFetchRate(MBeanServerConnection mbsc) {
        try {
            ObjectName objectName = new ObjectName("kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=Replica") ;
            MBeanInfo mbeanInfo = mbsc.getMBeanInfo(objectName) ;
            MBeanAttributeInfo[] mBeanAttributeInfos = mbeanInfo.getAttributes() ;
            for (MBeanAttributeInfo mBeanAttributeInfo :mBeanAttributeInfos) {
                System.out.println(mBeanAttributeInfo.toString());
            }
            JmxReporter.GaugeMBean gaugeMBean = MBeanServerInvocationHandler.newProxyInstance(mbsc,objectName, JmxReporter.GaugeMBean.class,true) ;
            Object o = gaugeMBean.getValue() ;
            System.out.println(o);
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

    private static void getMessagesInPerSec(MBeanServerConnection mbsc) {
        try {
            ObjectName objectName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec") ;
            AttributeList attributeList = mbsc.getAttributes(objectName,new String[]{"Count", "FifteenMinuteRate", "FiveMinuteRate", "OneMinuteRate", "MeanRate"}) ;
            List<Attribute> list = attributeList.asList() ;
            for (Attribute attribute:list) {
                Object o = attribute.getValue() ;
                String name = attribute.getName() ;
                if (o.getClass()==Long.class){
                    BigDecimal bigDecimal = new BigDecimal((Long)o) ;
                    System.out.println(name+":"+bigDecimal.toString());
                }
                if (o.getClass()==Double.class){
                    BigDecimal bigDecimal = new BigDecimal((Double)o) ;
                    System.out.println(name+":"+bigDecimal.toString());
                }
            }
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (InstanceNotFoundException e) {
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
