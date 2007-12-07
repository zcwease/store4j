package com.taobao.common.store.journal;

import java.io.IOException;

public interface JournalStoreMBean {

	String getDataFilesInfo();

	String getLogFilesInfo();

	int getNumber();

	/**
	 * @return the path
	 */
	String getPath();

	/**
	 * @return the name
	 */
	String getName();

	String getDataFileInfo();

	String getLogFileInfo();

	String viewIndexMap();
	
	long getSize() throws IOException;

}