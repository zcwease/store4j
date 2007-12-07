/**
 * 
 */
package com.taobao.common.store.journal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.taobao.common.cs.common.BytesKey;
import com.taobao.common.cs.common.Util;
import com.taobao.common.store.Store;

/**
 * key必须是16字节
 * 1、数据文件和日志文件在一起，不记录索引文件
 * 	 name.1 name.1.log
 * 2、data为真正的数据，顺序存放，使用引用计数
 * 3、log为操作+key+偏移量
 * 4、添加数据时，先添加name.1，获得offset和length，然后记录日志，增加引用计数，然后加入或更新内存索引
 * 5、删除数据时，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件
 * 6、获取数据时，直接从内存索引获得数据偏移量
 * 7、更新数据时，调用添加
 * 8、启动时，遍历每一个log文件，通过日志的操作恢复内存索引
 * 
 * @author dogun
 */
public class JournalStore implements Store, JournalStoreMBean {
	static Logger log = Logger.getLogger(JournalStore.class);
	
	static final int FILE_SIZE = 1024 * 1024 * 50; //20M
	
	private String path;
	private String name;
	
	private Map<BytesKey, OpItem> indexs = new ConcurrentHashMap<BytesKey, OpItem>(10000, 0.8F, 40);
	private Map<Integer, DataFile> dataFiles = new ConcurrentHashMap<Integer, DataFile>();
	private Map<Integer, LogFile> logFiles = new ConcurrentHashMap<Integer, LogFile>();
	
	private DataFile dataFile = null;
	private LogFile logFile = null;
	private AtomicInteger number = new AtomicInteger(0);
	
	private ReentrantLock addLock = new ReentrantLock();
	
	public JournalStore(String path, String name) throws IOException {
		Util.registMBean(this, name);
		this.path = path;
		this.name = name;
		
		addLock.lock();
		try {
			initLoad();
			//如果当前没有可用文件，生成
			if (null == this.dataFile || null == this.logFile) {
				newDataFile();
			}
			//准备好了
		} finally {
			addLock.unlock();
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					close();
				} catch (IOException e) {
					log.error("close error", e);
				}
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#add(byte[], byte[])
	 */
	public void add(byte[] key, byte[] data) throws IOException {
		//先检查是否已经存在，如果已经存在抛出异常 判断文件是否满了，添加name.1，获得offset，记录日志，增加引用计数，加入或更新内存索引
		checkParam(key, data);
		addLock.lock();
		try {
			innerAdd(key, data);
		} finally {
			addLock.unlock();
		}
	}

	/**
	 * @param key
	 * @param data
	 * @throws IOException
	 */
	private void innerAdd(byte[] key, byte[] data)
			throws IOException {
		BytesKey k = new BytesKey(key);
		if (this.indexs.containsKey(k)) {
			throw new IOException("发现重复的key");
		}
		if (this.dataFile.getLength() >= FILE_SIZE) { //满了
			newDataFile();
		}
		
		int num = this.number.get();
		DataFile df = this.dataFile;
		LogFile lf = this.logFile;
		
		if (null != df && null != lf) {
			long pos = df.write(ByteBuffer.wrap(data));
			OpItem op = new OpItem();
			op.key = key;
			op.length = data.length;
			op.offset = pos - op.length;
			op.op = OpItem.OP_ADD;
			op.number = num;
			lf.write(ByteBuffer.wrap(op.toByte()));
			df.increment();
			this.indexs.put(k, op);
		} else {
			throw new IOException("文件在使用的同时被删除了:" + num);
		}
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#get(byte[])
	 */
	public byte[] get(byte[] key) throws IOException {
		OpItem op = this.indexs.get(new BytesKey(key));
		byte[] data = null;
		if (null != op) {
			DataFile df = this.dataFiles.get(op.number);
			if (null != df) {
				ByteBuffer bf = ByteBuffer.wrap(new byte[(int)op.length]);
				df.read(bf, op.offset);
				data = bf.array();
			} else {
				log.warn("数据文件丢失：" + op);
			}
		}
		return data;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#iterator()
	 */
	public Iterator<byte[]> iterator() throws IOException {
		final Iterator<BytesKey> it = this.indexs.keySet().iterator();
		return new Iterator<byte[]>() {
			public boolean hasNext() {
				return it.hasNext();
			}
			public byte[] next() {
				BytesKey bk = it.next();
				if (null != bk) {
					return bk.getData();
				}
				return null;
			}
			public void remove() {
				throw new UnsupportedOperationException("不支持删除，请直接调用store.remove方法");
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#remove(byte[])
	 */
	public boolean remove(byte[] key) throws IOException {
		//获得记录在那个文件，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件
		boolean ret = false;
		addLock.lock();
		try {
			ret = innerRemove(key);
		} finally {
			addLock.unlock();
		}
		return ret;
	}

	/**
	 * @param key
	 * @return
	 * @throws IOException
	 */
	private boolean innerRemove(byte[] key) throws IOException {
		boolean ret = false;
		BytesKey k = new BytesKey(key);
		OpItem op = this.indexs.get(k);
		if (null != op) {
			LogFile lf = this.logFiles.get(op.number);
			DataFile df = this.dataFiles.get(op.number);
			if (null != lf && null != df) {
				OpItem o = new OpItem();
				o.key = key;
				o.length = op.length;
				o.number = op.number;
				o.offset = op.offset;
				o.op = OpItem.OP_DEL;
				lf.write(ByteBuffer.wrap(o.toByte()));
				df.decrement();
				this.indexs.remove(k);
				ret = true;
				//判断是否可以删了
				if (df.getLength() >= FILE_SIZE && df.isUnUsed()) {
					if (this.dataFile == df) { //判断如果是当前文件，生成新的
						newDataFile();
					}
					log.info("删除文件：" + df);
					this.dataFiles.remove(op.number);
					this.logFiles.remove(op.number);
					df.delete();
					lf.delete();
				}
			}
		}
		return ret;
	}
	
	/**
	 * @param key
	 * @param data
	 */
	private void checkParam(byte[] key, byte[] data) {
		if (null == key || null == data) throw new NullPointerException("key and data can't be null");
		if (key.length != 16) throw new IllegalArgumentException("key.length must be 16");
	}
	
	/**
	 * @throws FileNotFoundException
	 */
	private void newDataFile()
			throws IOException {
		int n = this.number.incrementAndGet();
		this.dataFile = new DataFile(new File(path + "/" + name + "." + n));
		this.logFile = new LogFile(new File(path + "/" + name + "." + n + ".log"));
		this.dataFiles.put(n, this.dataFile);
		this.logFiles.put(n, this.logFile);
		log.info("生成新文件：" + this.dataFile);
	}

	private void initLoad() throws IOException {
		log.warn("开始恢复数据");
		final String nm = name;
		File[] fs = new File(path).listFiles(new FilenameFilter() {
			public boolean accept(File dir, String n) {
				return n.startsWith(nm) && !n.endsWith(".log");
			}
		});
		log.warn("遍历每个数据文件");
		for (File f : fs) {
			try {
				log.warn("处理：" + f.getAbsolutePath());
				//获得number
				String fn = f.getName();
				int n = Integer.parseInt(fn.substring(name.length() + 1));
				if (n > this.number.get()) {
					this.number.set(n);
				}
				//保存本数据文件的索引信息
				Map<BytesKey, OpItem> idx = new HashMap<BytesKey, OpItem>();
				//生成dataFile和logFile
				DataFile df = new DataFile(f);
				LogFile lf = new LogFile(new File(f.getAbsolutePath() + ".log"));
				long size = lf.getLength() / OpItem.LENGTH;
				for (long i = 0; i < size; ++i) { //循环每一个操作
					ByteBuffer bf = ByteBuffer.wrap(new byte[OpItem.LENGTH]);
					lf.read(bf, i * OpItem.LENGTH);
					if (bf.hasRemaining()) {
						log.warn("log file error:" + lf + ", index:" + i);
						continue;
					}
					OpItem op = new OpItem();
					op.parse(bf.array());
					BytesKey key = new BytesKey(op.key);
					if (op.op == OpItem.OP_ADD) { //如果是添加的操作，加入索引，增加引用计数
						idx.put(key, op);
						df.increment();
					} else if (op.op == OpItem.OP_DEL) {//如果是删除的操作，索引去除，减少引用计数
						idx.remove(key);
						df.decrement();
					} else {
						log.warn("unknow op:" + (int)op.op);
					}
				}
				if (df.getLength() >= FILE_SIZE && df.isUnUsed()) { //如果这个数据文件已经达到指定大小，并且不再使用，删除
					df.delete();
					lf.delete();
					log.warn("不用了，也超过了大小，删除");
				} else { //否则加入map
					this.dataFiles.put(n, df);
					this.logFiles.put(n, lf);
					if (df.getLength() < FILE_SIZE) { //如果大小还没超过指定大小，当前数据文件就是这个
						if (null != this.dataFile || null != this.logFile) {
							throw new IOException("为什么超过有两个数据文件还小于fileSize呢？fileSize:" + FILE_SIZE);
						}
						this.dataFile = df;
						this.logFile = lf;
						log.warn("是当前文件");
					}
					if (!df.isUnUsed()) { //如果有索引，加入总索引 
						this.indexs.putAll(idx);
						log.warn("还在使用，放入索引，referenceCount:" + df.getReferenceCount() + ", index:" + idx.size());
					}
				}
			} catch (Exception e) {
				log.error("load data file error:" + f, e);
			}
		}
		log.warn("恢复数据：" + this.size());
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#size()
	 */
	public int size() throws IOException {
		return this.indexs.size();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#update(byte[], byte[])
	 */
	public boolean update(byte[] key, byte[] data) throws IOException {
		addLock.lock();
		try {
			if (innerRemove(key)) {
				innerAdd(key, data);
				return true;
			}
		} finally {
			addLock.unlock();
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFilesInfo()
	 */
	public String getDataFilesInfo() {
		return this.dataFiles.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFilesInfo()
	 */
	public String getLogFilesInfo() {
		return this.logFiles.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getNumber()
	 */
	public int getNumber() {
		return this.number.get();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getPath()
	 */
	public String getPath() {
		return path;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getName()
	 */
	public String getName() {
		return name;
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFileInfo()
	 */
	public String getDataFileInfo() {
		return this.dataFile.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFileInfo()
	 */
	public String getLogFileInfo() {
		return this.logFile.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#viewIndexMap()
	 */
	public String viewIndexMap() {
		return indexs.toString();
	}

	public void close() throws IOException {
		for (DataFile df : this.dataFiles.values()) {
			try {
				df.close();
			} catch (Exception e) {
				log.warn("close error:" + df, e);
			}
		}
		this.dataFiles.clear();
		for (LogFile lf : this.logFiles.values()) {
			try {
				lf.close();
			} catch (Exception e) {
				log.warn("close error:" + lf, e);
			}
		}
		this.logFiles.clear();
		this.dataFile = null;
		this.logFile = null;
	}

	public long getSize() throws IOException {
		return size();
	}
}
