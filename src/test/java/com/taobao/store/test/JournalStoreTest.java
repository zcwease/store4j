package com.taobao.store.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.util.UniqId;

public class JournalStoreTest {
    JournalStore store = null;

    byte[] key = null;

    @Before
    public void setUp() throws Exception {
        String path = "/tmp/journalStore-test";
        if (!(new File(path).exists())) (new File(path)).mkdirs();
        this.store = new JournalStore(path, "testStore");
        this.key = UniqId.getInstance().getUniqIDHash();
        Iterator<byte[]> it = this.store.iterator();
        while (it.hasNext()) { //清空上次的
            this.store.remove(it.next());
        }
        assertEquals(0, this.store.size());
    }

    @Test
    public void testAddGetRemoveMixed() throws Exception {
        long s = System.currentTimeMillis();
        for (int k = 0; k < 10000; ++k) {
            this.store.add(key, "hellofdfdfdfdfd".getBytes());
            byte[] data = this.store.get(key);
            assertNotNull(data);
            assertEquals("hellofdfdfdfdfd", new String(data));
            assertEquals(1, store.size());
            this.store.remove(key);
            assertEquals(0, store.size());
            data = this.store.get(key);
            assertNull(data);
            assertEquals(0, store.size());
        }
        System.out.println((System.currentTimeMillis() - s) + "ms");
    }

    static byte[] getId(int id, int seq) {
        final byte tmp[] = new byte[16];
        tmp[0] = (byte) ((0xff000000 & id) >> 24);
        tmp[1] = (byte) ((0xff0000 & id) >> 16);
        tmp[2] = (byte) ((0xff00 & id) >> 8);
        tmp[3] = (byte) (0xff & id);

        tmp[4] = (byte) ((0xff000000 & seq) >> 24);
        tmp[5] = (byte) ((0xff0000 & seq) >> 16);
        tmp[6] = (byte) ((0xff00 & seq) >> 8);
        tmp[7] = (byte) (0xff & seq);
        return tmp;
    }

    private String getMsg() {
        return "The quick brown fox jumps over the lazy dog, "
                + "The quick brown fox jumps over the lazy dog, "
                + "The quick brown fox jumps over the lazy dog, "
                + "The quick brown fox jumps over the lazy dog";
    }

    private class MsgCreator extends Thread {
        int id;

        int total;

        MsgCreator(int id, int total) {
            this.id = id;
            this.total = total;
        }

        public void run() {
            for (int k = 0; k < total; k++) {
                try {
                    store.add(JournalStoreTest.getId(id, k), getMsg()
                            .getBytes());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private class MsgRemover extends Thread {
        int id;

        int total;

        MsgRemover(int id, int total) {
            this.id = id;
            this.total = total;
        }

        public void run() {
            for (int k = 0; k < total;) {
                try {
                    if (store.get(JournalStoreTest.getId(id, k)) == null) {
                        continue;
                    }
                    boolean success = store.remove(JournalStoreTest
                            .getId(id, k));
                    if (!success) {
                        throw new IllegalStateException();
                    }
                    k++;
                    System.out.println("remove[" + id + ":" + k + "]");
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @Test
    public void testLoadHeavy() throws Exception {
        load(15, 10000);
    }

    @Test
    public void testLoadMedium() throws Exception {
        load(6, 10000);
    }

    @Test
    public void testLoadMin() throws Exception {
        load(3, 10000);
    }

    public void load(int num, int total) throws Exception {
        long s = System.currentTimeMillis();
        MsgCreator[] mcs = new MsgCreator[num];
        MsgRemover[] mrs = new MsgRemover[mcs.length];
        for (int i = 0; i < mcs.length; ++i) {
            MsgCreator mc = new MsgCreator(i, total);
            mcs[i] = mc;
            mc.start();
            MsgRemover mr = new MsgRemover(i, total);
            mrs[i] = mr;
            mr.start();
        }

        for (int i = 0; i < mcs.length; ++i) {
            mcs[i].join();
            mrs[i].join();
        }

        assertEquals(0, store.size());
        System.out.println((System.currentTimeMillis() - s) + "ms");
    }
}
