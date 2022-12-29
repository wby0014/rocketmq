/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;

public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 该方法在Master收到从服务器的拉取请求后被调用，表示从服务器当前已同步的偏移量，既然收到从服务器的反馈信息，需要唤醒某些消息发送者线程。
     * 如果从服务器收到的确认偏移量大于push2SlaveMaxOffset，则更新push2SlaveMaxOffset，
     * 然后唤醒GroupTransferService线程，各消息发送者线程再次判断自己本次发送的消息是否已经成功复制到从服务器。
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        // Broker服务监听套接字（本地IP+端口号）。
        private final SocketAddress socketAddressListen;
        // 服务端Socket通道，基于NIO。
        private ServerSocketChannel serverSocketChannel;
        // 事件选择器，基于NIO
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            /**
             * 创建ServerSocketChannel、创建Selector、设置TCP reuseAddress、绑定监听端口、设置为非阻塞模式，并注册OP_ACCEPT（连接事件）
             */
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 该方法是标准的基于NIO的服务端程式实例，选择器每1s处理一次连接就绪事件。
         * 连接事件就绪后，调用ServerSocketChannel的accept（）方法创建SocketChannel。
         * 然后为每一个连接创建一个HAConnection对象，该HAConnection将负责M-S数据同步逻辑
         *
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService主从同步阻塞实现，如果是同步主从模式，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器，
     * 从服务器数据的复制是在另外一个线程HAConnection中去拉取，所以消息发送者在这里需要等待数据传输的结果，
     * Group TransferService就是实现该功能，该类的整体结构与同步刷盘实现类（Commit Log$-GroupCommitService）类似
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

        public void putRequest(final CommitLog.GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        /**
         * GroupTransferService的职责是负责当主从同步复制结束后通知由于等待HA同步结果而阻塞的消息发送者线程。
         * 判断主从同步是否完成的依据是Slave中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，
         * 如果是则表示主从同步复制已经完成，唤醒消息发送线程，否则等待1s再次判断，每一个任务在一批任务中循环判断5次。
         * 消息发送者返回有两种情况：
         * 等待超过5s或GroupTrans fer Service通知主从复制完成。可以通过syncFlushTimeout来设置发送线程等待超时时间。Group-TransferService通知主从复制的实现如下。
         */
        private void doWaitTransfer() {
            if (!this.requestsRead.isEmpty()) {
                for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    long deadLine = req.getDeadLine();
                    while (!transferOK && deadLine - System.nanoTime() > 0) {
                        this.notifyTransferObject.waitForRunning(1000);
                        transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    }

                    req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }

                this.requestsRead = new LinkedList<>();
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    class HAClient extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;
        private long lastWriteTimestamp = System.currentTimeMillis();

        private long currentReportedOffset = 0;
        private int dispatchPosition = 0;
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * Step2：判断是否需要向Master反馈当前待拉取偏移量，Master与Slave的HA心跳发送间隔默认为5s，可通过配置haSendHeartbeatInterval来改变心跳间隔。
         * @return
         */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * Step3：向Master服务器反馈拉取偏移量。这里有两重意义，对于Slave端来说，是发送下次待拉取消息偏移量，而对于Master服务端来说，
         * 既可以认为是Slave本次请求拉取的消息偏移量，也可以理解为Slave的消息同步ACK确认消息。
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            /**
             * 这里RocketMQ作者提供了一个基于NIO的网络写示例程序：首先先将ByteBuffer的position设置为0, limit设置为待写入字节长度，
             * 然后调用putLong将待拉取偏移量写入ByteBuffer中，需要将ByteBuffer从写模式切换到读模式，这里的用法是手动将position设置为0,
             * limit设置为可读长度，其实这里可以直接调用ByteBuffer的flip（）方法来切换ByteBuffer的读写状态。
             * 特别需要留意的是，调用网络通道的write方法是在一个while循环中反复判断byteBuffer是否全部写入到通道中，这是由于NIO是一个非阻塞IO，调用一次write方法不一定会将ByteBuffer可读字节全部写入。
             */
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * Step5：处理网络读请求，即处理从Master服务器传回的消息数据。
         * 同样RocketMQ作者给出了一个处理网络读的NIO示例。循环判断readByteBuffer是否还有剩余空间，如果存在剩余空间，
         * 则调用SocketChannel#read（ByteBuffer readByteBuffer），将通道中的数据读入到读缓存区中
         * @return
         */
        private boolean processReadEvent() {
            /**
             * 1）如果读取到的字节数大于0，重置读取到0字节的次数，并更新最后一次写入时间戳（lastWriteTimestamp），
             *    然后调用dispatchReadRequest方法将读取到的所有消息全部追加到消息内存映射文件中，然后再次反馈拉取进度给服务器。
             * 2）如果连续3次从网络通道读取到0个字节，则结束本次读，返回true。
             * 3）如果读取到的字节数小于0或发生IO异常，则返回false
             */
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = byteBufferRead.array();
                        int dataStart = this.dispatchPosition + msgHeaderSize;

                        HAService.this.defaultMessageStore.appendToCommitLog(
                                masterPhyOffset, bodyData, dataStart, bodySize);

                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * Step1:Slave服务器连接Master服务器。如果socketChannel为空，则尝试连接Master。
         * 如果master地址为空，返回false；如果master地址不为空，则建立到Master的TCP连接，然后注册OP_READ（网络读事件），
         * 初始化currentReportedOffset为commitlog文件的最大偏移量、lastWriteTimestamp上次写入时间戳为当前时间戳，并返回true。
         * 在Broker启动时，如果Broker角色为SLAVE时将读取Broker配置文件中的haMasterAddress属性并更新HAClient的masterAddrees，
         * 如果角色为SLAVE并且haMasterAddress为空，启动并不会报错，但不会执行主从同步复制，该方法最终返回是否成功连接上Master
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * HAClient线程反复执行下述5个步骤完成主从同步复制功能
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // step1
                    if (this.connectMaster()) {
                        // step2
                        if (this.isTimeToReportOffset()) {
                            // step3
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }
                        // step4 进行事件选择，其执行间隔1s
                        this.selector.select(1000);
                        // step5 处理网络读请求，即处理从Master服务器传回的消息数据。
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
