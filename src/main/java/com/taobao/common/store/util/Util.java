/**
 * 
 */
package com.taobao.common.store.util;


/**
 * @author yxq1871
 *
 */
public class Util {
    /**
     * ��һ������ע���MBeanServer
     * @param o
     */
    public static void registMBean(Object o, String name) {
    	MyMBeanServer.getInstance().registMBean(o, name);
    }
}
