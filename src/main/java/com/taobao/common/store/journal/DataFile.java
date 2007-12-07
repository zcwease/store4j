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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ������һ�������ļ�
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
class DataFile {
	private File file;
	private AtomicInteger referenceCount = new AtomicInteger(0);
	private FileChannel fc;

	/**
	 * ���캯�������ָ�����ļ������ҽ�ָ��ָ���ļ���β
	 * @param file
	 * @throws IOException
	 */
	DataFile(File file) throws IOException {
		this.file = file;
		fc = new RandomAccessFile(file, "rw").getChannel();
		//ָ���Ƶ����
		fc.position(fc.size());
	}

	/**
	 * ����ļ��Ĵ�С
	 * 
	 * @return �ļ��Ĵ�С
	 * @throws IOException
	 */
	long getLength() throws IOException {
		return fc.size();
	}

	/**
	 * ɾ���ļ�
	 * @return �Ƿ�ɾ���ɹ�
	 * @throws IOException
	 */
	boolean delete() throws IOException {
		close();
		return file.delete();
	}

	/**
	 * ǿ�ƽ�����д��Ӳ��
	 * 
	 * @throws IOException
	 */
	void force() throws IOException {
		fc.force(true);
	}

	/**
	 * �ر��ļ�
	 * 
	 * @throws IOException
	 */
	void close() throws IOException {
		fc.close();
	}
	
	/**
	 * ���ļ���ȡ���ݵ�bf��ֱ���������߶����ļ���β��
	 * <br />
	 * �ļ���ָ�������ƶ�bf�Ĵ�С
	 * 
	 * @param bf
	 * @throws IOException
	 */
	void read(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.read(bf);
			if (l < 0) break;
		}
	}

	/**
	 * ���ļ����ƶ�λ�ö�ȡ���ݵ�bf��ֱ���������߶����ļ���β��
	 * <br />
	 * �ļ�ָ�벻���ƶ�
	 * 
	 * @param bf
	 * @param offset
	 * @throws IOException
	 */
	void read(ByteBuffer bf, long offset) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.read(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	/**
	 * д��bf���ȵ����ݵ��ļ����ļ�ָ�������ƶ�
	 * @param bf
	 * @return д�����ļ�position
	 * @throws IOException
	 */
	long write(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.write(bf);
			if (l < 0) break;
		}
		return fc.position();
	}
	
	/**
	 * ��ָ��λ��д��bf���ȵ����ݵ��ļ����ļ�ָ��<b>����</b>����ƶ�
	 * @param offset
	 * @param bf
	 * @throws IOException
	 */
	void write(long offset, ByteBuffer bf) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.write(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	/**
	 * ���ļ�����һ�����ü���
	 * @return ���Ӻ�����ü���
	 */
	int increment() {
		return referenceCount.incrementAndGet();
	}

	/**
	 * ���ļ�����һ�����ü���
	 * @return ���ٺ�����ü���
	 */
	int decrement() {
		return referenceCount.decrementAndGet();
	}

	/**
	 * �ļ��Ƿ���ʹ�ã����ü����Ƿ���0�ˣ�
	 * @return �ļ��Ƿ���ʹ��
	 */
	boolean isUnUsed() {
		return getReferenceCount() <= 0;
	}
	
	/**
	 * ������ü�����ֵ
	 * @return ���ü�����ֵ
	 */
	int getReferenceCount() {
		return this.referenceCount.get();
	}

	@Override
	public String toString() {
		String result = null;
		try {
			result = file.getName() + " , length = " + getLength()
					+ " refCount = " + referenceCount + " position:" + fc.position();
		} catch (IOException e) {
			result = e.getMessage();
		}
		return result;
	}
}
