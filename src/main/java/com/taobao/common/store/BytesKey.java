/**
 * 
 */
package com.taobao.common.store;

import java.io.Serializable;

/**
 * @author dogun
 *
 */
public class BytesKey implements Serializable {
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -6296965387124592707L;

	private byte[] data;
	
	public BytesKey(byte[] data) {
		this.data = data;
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(byte[] data) {
		this.data = data;
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		if (null != this.data) {
	        for (int i = 0; i < this.data.length; i++) {
	            h = 31*h + data[i++];
	        }
		}
        return h;
	}
	
	@Override
	public boolean equals(Object o) {
		if (null == o || !(o instanceof BytesKey)) {
			return false;
		}
		BytesKey k = (BytesKey)o;
		if (null == k.getData() && null == this.getData()) {
			return true;
		}
		if (null == k.getData() || null == this.getData()) {
			return false;
		}
		if (k.getData().length != this.getData().length) {
			return false;
		}
		for (int i = 0; i < this.data.length; ++i) {
			if (this.data[i] != k.getData()[i]) {
				return false;
			}
		}
		return true;
	}
}
