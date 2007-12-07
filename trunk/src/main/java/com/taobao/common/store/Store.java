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
package com.taobao.common.store;

import java.io.IOException;
import java.util.Iterator;

/**
 * <b>�洢�Ľӿ�</b>
 * <p>�򵥵Ĵ洢����byte[]���͵�key value��֧�֣�
 * ��֧��add remove update�������ݲ���</p>
 * 
 * @author dogun (yuexuqiang at gmail.com)
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
	 * @return ��õ����ݣ����û�У�����null
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
	 * @return ���ݸ���
	 * @throws IOException
	 */
	int size() throws IOException;
	
	/**
	 * ����key
	 * @return key�ı�����
	 * @throws IOException
	 */
	Iterator<byte[]> iterator() throws IOException;
	
	/**
	 * �رմ洢
	 * @throws IOException
	 */
	void close() throws IOException;
}
