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
	 * 添加一个数据
	 * @param key
	 * @param data
	 * @throws IOException
	 */
	void add(byte[] key, byte[] data) throws IOException;
	
	/**
	 * 删除一个数据
	 * @param key
	 * @return 是否删除了数据
	 * @throws IOException
	 */
	boolean remove(byte[] key) throws IOException;
	
	/**
	 * 获取一个数据
	 * @param key
	 * @return
	 * @throws IOException
	 */
	byte[] get(byte[] key) throws IOException;
	
	/**
	 * 更新一个数据
	 * @param key
	 * @param data
	 * @return 是否有更新到
	 * @throws IOException
	 */
	boolean update(byte[] key, byte[] data) throws IOException;
	
	/**
	 * 获取数据个数
	 * @return
	 * @throws IOException
	 */
	int size() throws IOException;
	
	/**
	 * 遍历key
	 * @return
	 * @throws IOException
	 */
	Iterator<byte[]> iterator() throws IOException;
	
	/**
	 * 關閉
	 * @throws IOException
	 */
	void close() throws IOException;
}
