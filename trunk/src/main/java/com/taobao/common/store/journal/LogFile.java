package com.taobao.common.store.journal;

import java.io.File;
import java.io.IOException;


/**
 * һ�������ļ�
 * 
 * @author dogun
 */
class LogFile extends DataFile {
	LogFile(File file) throws IOException {
		super(file);
	}
}
