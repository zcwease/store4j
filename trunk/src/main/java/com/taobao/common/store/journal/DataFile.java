package com.taobao.common.store.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个数据文件
 * 
 * @author dogun
 */
class DataFile {
	private File file;
	private AtomicInteger referenceCount = new AtomicInteger(0);
	private FileChannel fc;

	DataFile(File file) throws IOException {
		this.file = file;
		fc = new RandomAccessFile(file, "rw").getChannel();
		//指针移到最后
		fc.position(fc.size());
	}

	long getLength() throws IOException {
		return fc.size();
	}

	boolean delete() throws IOException {
		close();
		return file.delete();
	}

	void force() throws IOException {
		fc.force(true);
	}

	void close() throws IOException {
		fc.close();
	}
	
	void read(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.read(bf);
			if (l < 0) break;
		}
	}

	void read(ByteBuffer bf, long offset) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.read(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	/**
	 * 写入数据
	 * @param bf
	 * @return 写入后的文件position
	 * @throws IOException
	 */
	long write(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.write(bf);
			if (l < 0) break;
		}
		return fc.position();
	}
	
	void write(long offset, ByteBuffer bf) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.write(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	int increment() {
		return referenceCount.incrementAndGet();
	}

	int decrement() {
		return referenceCount.decrementAndGet();
	}

	boolean isUnUsed() {
		return getReferenceCount() <= 0;
	}
	
	int getReferenceCount() {
		return this.referenceCount.get();
	}

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
