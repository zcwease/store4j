/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package com.taobao.common.store.journal;

import java.nio.ByteBuffer;


/**
 * һ����־��¼ ����+����key+�����ļ����+ƫ����+����
 * 
 * @author dogun (yuexuqiang at gmail.com)
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
	
	/**
	 * ��һ������ת�����ֽ�����
	 * 
	 * @return �ֽ�����
	 */
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

	/**
	 * ͨ���ֽ����鹹���һ��������־
	 * @param data
	 */
	void parse(byte[] data) {
		ByteBuffer bf = ByteBuffer.wrap(data);
		key = new byte[16];
		bf.get(key);
		op = bf.get();
		number = bf.getInt();
		offset = bf.getLong();
		length = bf.getInt();
	}
	
	@Override
	public String toString() {
		return "OpItem number:" + number + ", op:" + (int)op + ", offset:" + offset + ", length:" + length;
	}
}
