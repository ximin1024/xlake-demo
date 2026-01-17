/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.backend.wal;

import java.nio.ByteBuffer;

public class DefaultWALRecord implements WALRecord {
    private final long logSequenceNum;
    private final byte[] data;
    private final long timestamp;
    private final long commitId;
    private final String uniqTableIdentifier;
    private final String marker;

    public DefaultWALRecord(long logSequenceNum, byte[] data, long timestamp,
                            long commitId, String uniqTableIdentifier, String marker) {
        this.logSequenceNum = logSequenceNum;
        this.data = data;
        this.timestamp = timestamp;
        this.commitId = commitId;
        this.uniqTableIdentifier = uniqTableIdentifier;
        this.marker = marker;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(
                length()
        );
        buffer.putInt(uniqTableIdentifier.getBytes().length);
        buffer.put(uniqTableIdentifier.getBytes());
        buffer.putLong(commitId);
        buffer.putLong(logSequenceNum);
        buffer.putInt(marker.getBytes().length);
        buffer.put(marker.getBytes());
        buffer.putLong(timestamp);
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }

    @Override
    public long sequence() {
        return logSequenceNum;
    }

    @Override
    public String marker() {
        return marker;
    }

    public static DefaultWALRecord deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int tableIdentifierLength = buffer.getInt();
        byte[] tableIdentifierBytes = new byte[tableIdentifierLength];
        buffer.get(tableIdentifierBytes);
        long commitId = buffer.getLong();
        long lsn = buffer.getLong();
        int markerLength = buffer.getInt();
        byte[] markerBytes = new byte[markerLength];
        buffer.get(markerBytes);
        long timestamp = buffer.getLong();
        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);
        return new DefaultWALRecord(lsn, data, timestamp, commitId, new String(tableIdentifierBytes), new String(markerBytes));
    }


    @Override
    public int length() {
        return 4 + uniqTableIdentifier.getBytes().length + 8 +
                8 + 4 + marker.getBytes().length + 8 + 4 + data.length;
    }

    @Override
    public byte[] value() {
        return data;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public long commitId() {
        return commitId;
    }

    @Override
    public String uniqTableIdentifier() {
        return uniqTableIdentifier;
    }
}
