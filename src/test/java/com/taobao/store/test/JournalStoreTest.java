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
 * @author xalinx at gmail dot com
 * @date 2007-12-10
 *
 */
public class JournalStoreTest {
    JournalStore store = null;

    @Before
    public void setUp() throws Exception {
        String path = "\\home\\admin\\tmp\\notify-store-test\\";
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
    public void testAdd() throws Exception {
        int num = 100000;
        long s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.add(getId(k, k), getMsg().getBytes());
        }
        s = System.currentTimeMillis() - s;
        System.out.println("add " + num + " waste " + s + "ms, average " + s
                * 1.0d / num);
        assertEquals(num, store.size());

        //load remove
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.remove(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("remove " + num + " waste " + s + "ms, average " + s
                * 1.0d / num);
        assertEquals(0, store.size());
    }

    @Test
    public void testLoad9() throws Exception {
        load(8, 1000);
    }

    @Test
    public void testLoad1() throws Exception {
        load(2, 1000);
    }

    public void load(int ThreadNum, int totalPerThread) throws Exception {
        MsgCreator[] mcs = new MsgCreator[ThreadNum];
        MsgRemover[] mrs = new MsgRemover[mcs.length];
        for (int i = 0; i < mcs.length; i++) {
            MsgCreator mc = new MsgCreator(i, totalPerThread);
            mcs[i] = mc;
            mc.start();
            MsgRemover mr = new MsgRemover(i, totalPerThread);
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

    private String getMsg() {
        return "The quick brown fox jumps over the lazy dog";
    }

    private class MsgCreator extends Thread {
        int id;

        int total;

        long timeTotal;

        MsgCreator(int id, int total) {
            this.id = id;
            this.total = total;
        }

        public void run() {
            for (int k = 0; k < total; k++) {
                try {
                    long start = System.currentTimeMillis();
                    store.add(JournalStoreTest.getId(id, k), getMsg()
                            .getBytes());
                    timeTotal += System.currentTimeMillis() - start;
                    Thread.sleep(10);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private class MsgRemover extends Thread {
        int id;

        int total;

        long timeTotal;

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
                    long start = System.currentTimeMillis();
                    boolean success = store.remove(JournalStoreTest
                            .getId(id, k));
                    timeTotal += System.currentTimeMillis() - start;
                    if (!success) {
                        throw new IllegalStateException();
                    }
                    k++;
                    Thread.sleep(10);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
