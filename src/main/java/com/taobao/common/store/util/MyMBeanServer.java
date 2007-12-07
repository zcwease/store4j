/**
 * 
 */
package com.taobao.common.store.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

/**
 * @author dogun
 *
 */
public final class MyMBeanServer {
	private static final Logger log = Logger.getLogger(MyMBeanServer.class);
	private MBeanServer mbs = null;

	private static MyMBeanServer me = new MyMBeanServer();
	private MyMBeanServer() {
	    //´´½¨MBServer
		String hostName = null;
		 try {
             InetAddress localhost = InetAddress.getLocalHost();

             hostName        = localhost.getHostName();
         } catch (UnknownHostException e) {
             hostName        = "localhost";
         }
    	try {
    		boolean useJmx = Boolean.parseBoolean(System.getProperty("notify.useJMX", "false"));
    		if (useJmx) {
    			mbs = ManagementFactory.getPlatformMBeanServer();
	    		int port = Integer.parseInt(System.getProperty("notify.rmi.port", "6669"));
	    		String rmiName = System.getProperty("notify.rmi.name", "notifyServer");
	    		Registry reg = null;
	    		try {
	    			reg = LocateRegistry.getRegistry(port);
	    			reg.list();
	    		}catch (Exception e) {
	    			reg = null;
	    		}
	    		if (null == reg) {
	    			reg = LocateRegistry.createRegistry(port);
	    		}
	    		reg.list();
	    		String serverURL = "service:jmx:rmi://"+hostName+"/jndi/rmi://"+hostName+":"+port+"/" + rmiName;
	    		JMXServiceURL url = new JMXServiceURL(serverURL);
				final JMXConnectorServer connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
				connectorServer.start();
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						try {
							System.err.println("JMXConnector stop");
							connectorServer.stop();
						} catch (IOException e) {
							log.error("stop error", e);
						}
					}
				});
				log.warn("jmx url: " + serverURL);
    		}
		} catch (Exception e) {
			log.error("create MBServer error", e);
		}
	}
	
	public static MyMBeanServer getInstance() {
		return me;
	}
	
	public void registMBean(Object o, String name) {
		//×¢²áMBean
		if (null != mbs) {
			try {
				mbs.registerMBean(o, new ObjectName(o.getClass().getPackage().getName() + 
						":type=" + o.getClass().getSimpleName() 
						+ (null == name ? (",id=" + o.hashCode()) : (",name=" + name + "-" + o.hashCode()))));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
