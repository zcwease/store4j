/**
 * 
 */
package com.taobao.common.store;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author dogun
 *
 */
public interface Store {
	/**
	 * ���һ������
	 * @param key
	 * @param data
	 * @throws IOException
	 */
	void add(byte[] key, byte[] data) throws IOException;
	
	/**
	 * ɾ��һ������
	 * @param key
	 * @return �Ƿ�ɾ��������
	 * @throws IOException
	 */
	boolean remove(byte[] key) throws IOException;
	
	/**
	 * ��ȡһ������
	 * @param key
	 * @return
	 * @throws IOException
	 */
	byte[] get(byte[] key) throws IOException;
	
	/**
	 * ����һ������
	 * @param key
	 * @param data
	 * @return �Ƿ��и��µ�
	 * @throws IOException
	 */
	boolean update(byte[] key, byte[] data) throws IOException;
	
	/**
	 * ��ȡ���ݸ���
	 * @return
	 * @throws IOException
	 */
	int size() throws IOException;
	
	/**
	 * ����key
	 * @return
	 * @throws IOException
	 */
	Iterator<byte[]> iterator() throws IOException;
	
	/**
	 * �P�]
	 * @throws IOException
	 */
	void close() throws IOException;
}
