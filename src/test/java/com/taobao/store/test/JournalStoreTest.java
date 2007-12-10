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
package com.taobao.store.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.util.UniqId;

/**
 * @author dogun (yuexuqiang at gmail.com)
 * @author lin wang(xalinx at gmail dot com)
 * @date 2007-12-10
 */
public class JournalStoreTest {
    JournalStore store = null;

    @Before
    public void setUp() throws Exception {
        String path = "tmp" + File.pathSeparator + "notify-store-test";
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("can't make dir " + dir);
        }

        File[] fs = dir.listFiles();
        for (File f : fs) {
            if (!f.delete()) {
                throw new IllegalStateException("can't delete " + f);
            }
        }

        this.store = new JournalStore(path, "testStore");
        assertEquals(0, this.store.size());
    }

    @After
    public void after() throws IOException {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void testAddGetRemoveMixed() throws Exception {
        long s = System.currentTimeMillis();
        byte[] key = UniqId.getInstance().getUniqIDHash();
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

    @Test
    public void testLoadAddReadRemove10K() throws Exception {
        loadAddReadRemove(getMsg10K());
    }

    @Test
    public void testLoadAddReadRemove1K() throws Exception {
        loadAddReadRemove(getMsg1K());
    }

    public void loadAddReadRemove(String msg) throws Exception {
        int num = 100000;
        //load add
        long s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.add(getId(k, k), msg.getBytes());
        }
        s = System.currentTimeMillis() - s;
        System.out.println("add " + msg.getBytes().length + " bytes " + num
                + " times waste " + s + "ms, average " + s * 1.0d / num);
        assertEquals(num, store.size());

        //load read
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.get(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("get " + msg.getBytes().length + " bytes " + num
                + " times waste " + s + "ms, average " + s * 1.0d / num);

        //load remove
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.remove(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("remove " + num + " times waste " + s
                + "ms, average " + s * 1.0d / num);
        assertEquals(0, store.size());
    }

    @Test
    public void testLoadHeavy() throws Exception {
        load(8, 2000, 5);
    }

    @Test
    public void testLoadMin() throws Exception {
        load(2, 2000, 5);
    }

    public void load(int ThreadNum, int totalPerThread, long meantime)
            throws Exception {
        MsgCreator[] mcs = new MsgCreator[ThreadNum];
        MsgRemover[] mrs = new MsgRemover[mcs.length];
        for (int i = 0; i < mcs.length; i++) {
            MsgCreator mc = new MsgCreator(i, totalPerThread, meantime);
            mcs[i] = mc;
            mc.start();
            MsgRemover mr = new MsgRemover(i, totalPerThread, meantime);
            mrs[i] = mr;
            mr.start();
        }

        for (int i = 0; i < mcs.length; i++) {
            mcs[i].join();
            mrs[i].join();
        }

        assertEquals(0, store.size());

        long totalAddTime = 0;
        long totalRemoveTime = 0;
        for (int i = 0; i < mcs.length; i++) {
            totalAddTime += mcs[i].timeTotal;
            totalRemoveTime += mrs[i].timeTotal;
        }

        System.out.println(totalPerThread * ThreadNum * 2 + " of " + ThreadNum
                * 2 + " thread average: add " + totalAddTime * 1.0d
                / (totalPerThread * ThreadNum) + ", remove " + totalRemoveTime
                * 1.0d / (totalPerThread * ThreadNum));
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

    private static final byte[] MSG_BYTES = new byte[102400];

    private String getMsg1K() {
        return new String(MSG_BYTES, 0, 1024);
    }

    private String getMsg10K() {
        return new String(MSG_BYTES, 0, 10240);
    }

    private class MsgCreator extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;

        MsgCreator(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }

        public void run() {
            for (int k = 0; k < totalPerThread; k++) {
                try {
                    Thread.sleep(meantime);
                    long start = System.currentTimeMillis();
                    store.add(JournalStoreTest.getId(id, k), getMsg1K()
                            .getBytes());
                    timeTotal += System.currentTimeMillis() - start;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private class MsgRemover extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;

        MsgRemover(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }

        public void run() {
            for (int k = 0; k < totalPerThread;) {
                try {
                    Thread.sleep(meantime);
                    byte[] read = store.get(JournalStoreTest.getId(id, k));
                    if (read == null) {
                        continue;
                    }
                    long start = System.currentTimeMillis();
                    boolean success = store.remove(JournalStoreTest
                            .getId(id, k));
                    timeTotal += System.currentTimeMillis() - start;
                    if (!success) {
                        throw new IllegalStateException();
                    }
                    k++;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
