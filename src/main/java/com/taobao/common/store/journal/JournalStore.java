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
 * key������16�ֽ�
 * 1�������ļ�����־�ļ���һ�𣬲���¼�����ļ�
 * 	 name.1 name.1.log
 * 2��dataΪ���������ݣ�˳���ţ�ʹ�����ü���
 * 3��logΪ����+key+ƫ����
 * 4���������ʱ�������name.1�����offset��length��Ȼ���¼��־���������ü�����Ȼ����������ڴ�����
 * 5��ɾ������ʱ����¼��־��ɾ���ڴ������������ļ��������жϴ�С�Ƿ������С�ˣ������������ˣ���ɾ�������ļ�����־�ļ�
 * 6����ȡ����ʱ��ֱ�Ӵ��ڴ������������ƫ����
 * 7����������ʱ���������
 * 8������ʱ������ÿһ��log�ļ���ͨ����־�Ĳ����ָ��ڴ�����
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
			//�����ǰû�п����ļ�������
			if (null == this.dataFile || null == this.logFile) {
				newDataFile();
			}
			//׼������
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
		//�ȼ���Ƿ��Ѿ����ڣ�����Ѿ������׳��쳣 �ж��ļ��Ƿ����ˣ����name.1�����offset����¼��־���������ü��������������ڴ�����
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
			throw new IOException("�����ظ���key");
		}
		if (this.dataFile.getLength() >= FILE_SIZE) { //����
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
			throw new IOException("�ļ���ʹ�õ�ͬʱ��ɾ����:" + num);
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
				log.warn("�����ļ���ʧ��" + op);
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
				throw new UnsupportedOperationException("��֧��ɾ������ֱ�ӵ���store.remove����");
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#remove(byte[])
	 */
	public boolean remove(byte[] key) throws IOException {
		//��ü�¼���Ǹ��ļ�����¼��־��ɾ���ڴ������������ļ��������жϴ�С�Ƿ������С�ˣ������������ˣ���ɾ�������ļ�����־�ļ�
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
				//�ж��Ƿ����ɾ��
				if (df.getLength() >= FILE_SIZE && df.isUnUsed()) {
					if (this.dataFile == df) { //�ж�����ǵ�ǰ�ļ��������µ�
						newDataFile();
					}
					log.info("ɾ���ļ���" + df);
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
		log.info("�������ļ���" + this.dataFile);
	}

	private void initLoad() throws IOException {
		log.warn("��ʼ�ָ�����");
		final String nm = name;
		File[] fs = new File(path).listFiles(new FilenameFilter() {
			public boolean accept(File dir, String n) {
				return n.startsWith(nm) && !n.endsWith(".log");
			}
		});
		log.warn("����ÿ�������ļ�");
		for (File f : fs) {
			try {
				log.warn("����" + f.getAbsolutePath());
				//���number
				String fn = f.getName();
				int n = Integer.parseInt(fn.substring(name.length() + 1));
				if (n > this.number.get()) {
					this.number.set(n);
				}
				//���汾�����ļ���������Ϣ
				Map<BytesKey, OpItem> idx = new HashMap<BytesKey, OpItem>();
				//����dataFile��logFile
				DataFile df = new DataFile(f);
				LogFile lf = new LogFile(new File(f.getAbsolutePath() + ".log"));
				long size = lf.getLength() / OpItem.LENGTH;
				for (long i = 0; i < size; ++i) { //ѭ��ÿһ������
					ByteBuffer bf = ByteBuffer.wrap(new byte[OpItem.LENGTH]);
					lf.read(bf, i * OpItem.LENGTH);
					if (bf.hasRemaining()) {
						log.warn("log file error:" + lf + ", index:" + i);
						continue;
					}
					OpItem op = new OpItem();
					op.parse(bf.array());
					BytesKey key = new BytesKey(op.key);
					if (op.op == OpItem.OP_ADD) { //�������ӵĲ����������������������ü���
						idx.put(key, op);
						df.increment();
					} else if (op.op == OpItem.OP_DEL) {//�����ɾ���Ĳ���������ȥ�����������ü���
						idx.remove(key);
						df.decrement();
					} else {
						log.warn("unknow op:" + (int)op.op);
					}
				}
				if (df.getLength() >= FILE_SIZE && df.isUnUsed()) { //�����������ļ��Ѿ��ﵽָ����С�����Ҳ���ʹ�ã�ɾ��
					df.delete();
					lf.delete();
					log.warn("�����ˣ�Ҳ�����˴�С��ɾ��");
				} else { //�������map
					this.dataFiles.put(n, df);
					this.logFiles.put(n, lf);
					if (df.getLength() < FILE_SIZE) { //�����С��û����ָ����С����ǰ�����ļ��������
						if (null != this.dataFile || null != this.logFile) {
							throw new IOException("Ϊʲô���������������ļ���С��fileSize�أ�fileSize:" + FILE_SIZE);
						}
						this.dataFile = df;
						this.logFile = lf;
						log.warn("�ǵ�ǰ�ļ�");
					}
					if (!df.isUnUsed()) { //��������������������� 
						this.indexs.putAll(idx);
						log.warn("����ʹ�ã�����������referenceCount:" + df.getReferenceCount() + ", index:" + idx.size());
					}
				}
			} catch (Exception e) {
				log.error("load data file error:" + f, e);
			}
		}
		log.warn("�ָ����ݣ�" + this.size());
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
