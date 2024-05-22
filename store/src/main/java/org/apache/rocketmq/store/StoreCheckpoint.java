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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * checkpoint是检查点文件，保存broker最后正常存储各种数据的时间
 * Broker 在启动时会初始化 abort、checkpoint 两个文件。正常关闭进程时会删除 abort文件，将 checkpoint 文件刷盘
 * 异常关闭时，通常来不及删除 abort 文件。由此，在重新启动Broker时会根据abort判断是否需要异常停止进程，而后恢复数据
 */
public class StoreCheckpoint {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    // commitLog异步、同步刷盘成功后，更新这个时间，最后一条刷盘的commitLog存储时间
    private volatile long physicMsgTimestamp = 0;
    // consumeQueue刷盘成功、添加索引成功，更新这个时间，最后一条写consumeQueue消息的存储时间
    private volatile long logicsMsgTimestamp = 0;
    // Index file刷盘成功，更新这个时间，被刷盘的Index file的最后更新时间
    private volatile long indexMsgTimestamp = 0;

    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
        } else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
        this.mappedByteBuffer.force();
    }

    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0)
            min = 0;

        return min;
    }

    public long getIndexMsgTimestamp() {
        return indexMsgTimestamp;
    }

    public void setIndexMsgTimestamp(long indexMsgTimestamp) {
        this.indexMsgTimestamp = indexMsgTimestamp;
    }

}
