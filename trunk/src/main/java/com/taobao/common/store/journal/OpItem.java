/**
 * 
 */
package com.taobao.common.store.journal;

import java.nio.ByteBuffer;


/**
 * @author dogun
 *
 */
public class OpItem {
	static final byte OP_ADD = 1;
	static final byte OP_DEL = 2;
	
	static final int KEY_LENGTH = 16;
	static final int LENGTH = KEY_LENGTH + 1 + 4 + 8 + 4;
	
	byte op;
	byte[] key;
	int number;
	long offset;
	int length;
	
	byte[] toByte() {
		byte[] data = new byte[LENGTH];
		ByteBuffer bf = ByteBuffer.wrap(data);
		bf.put(key);
		bf.put(op);
		bf.putInt(number);
		bf.putLong(offset);
		bf.putInt(length);
		return bf.array();
	}

	void parse(byte[] data) {
		ByteBuffer bf = ByteBuffer.wrap(data);
		key = new byte[16];
		bf.get(key);
		op = bf.get();
		number = bf.getInt();
		offset = bf.getLong();
		length = bf.getInt();
	}
	
	public String toString() {
		return "OpItem number:" + number + ", op:" + (int)op + ", offset:" + offset + ", length:" + length;
	}
}
