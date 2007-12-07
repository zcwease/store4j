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

import java.io.IOException;

/**
 * ��־��ʽ�洢��MBean
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
public interface JournalStoreMBean {

	/**
	 * ���������Ч�����ļ�����Ϣ
	 * @return ������Ч�����ļ�����Ϣ
	 */
	String getDataFilesInfo();

	/**
	 * ���������Ч��־�ļ�����Ϣ
	 * @return ������Ч��־�ļ�����Ϣ
	 */
	String getLogFilesInfo();

	/**
	 * ��ȡ��ǰ���ļ����
	 * @return ��ǰ���ļ����
	 */
	int getNumber();

	/**
	 * ��ȡ�洢��·��
	 * @return the path
	 */
	String getPath();

	/**
	 * ��ȡ�洢������
	 * @return the name
	 */
	String getName();

	/**
	 * ��ȡ��ǰ�����ļ���Ϣ
	 * @return ��ǰ�����ļ���Ϣ
	 */
	String getDataFileInfo();

	/**
	 * ��ȡ��ǰ��־�ļ���Ϣ
	 * @return ��ǰ��־�ļ���Ϣ
	 */
	String getLogFileInfo();

	/**
	 * �鿴��������Ϣ��<b>ע��:</b>�ò������ܻ�ű��ڴ�
	 * @return ���е�������Ϣ
	 */
	String viewIndexMap();
	
	/**
	 * ������ݵĸ���
	 * @return ���ݵĸ���
	 * @throws IOException
	 */
	long getSize() throws IOException;
}