/**
 * 
 */
package com.taobao.common.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dogun
 *
 */
public class MemStore implements Store {
	private Map<BytesKey, byte[]> datas = new ConcurrentHashMap<BytesKey, byte[]>();
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#add(byte[], byte[])
	 */
	public void add(byte[] key, byte[] data) throws IOException {
		datas.put(new BytesKey(key), data);
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#get(byte[])
	 */
	public byte[] get(byte[] key) throws IOException {
		return datas.get(new BytesKey(key));
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#iterator()
	 */
	public Iterator<byte[]> iterator() throws IOException {
		final Iterator<BytesKey> it = datas.keySet().iterator();
		return new Iterator<byte[]>() {
			public boolean hasNext() {
				return it.hasNext();
			}

			public byte[] next() {
				BytesKey key = it.next();
				if (null == key) {
					return null;
				}
				return key.getData();
			}

			public void remove() {
				it.remove();
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#remove(byte[])
	 */
	public boolean remove(byte[] key) throws IOException {
		return null != datas.remove(new BytesKey(key));
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#size()
	 */
	public int size() throws IOException {
		return datas.size();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#update(byte[], byte[])
	 */
	public boolean update(byte[] key, byte[] data) throws IOException {
		datas.put(new BytesKey(key), data);
		return true;
	}

	public void close() throws IOException {
		//nodo
	}
}
