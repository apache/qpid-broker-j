/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.server.logging;

import java.util.Iterator;
import org.apache.qpid.server.configuration.BrokerProperties;

public class LogRecorder implements Iterable<LogRecorder.Record>
{
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private String _name;
    private long _recordId;

    private final int _bufferSize = Integer.getInteger(BrokerProperties.PROPERTY_LOG_RECORDS_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    private final int _mask = _bufferSize - 1;
    private Record[] _records = new Record[_bufferSize];


    public static class Record
    {
        private long _id;
        private String _logger;
        private long _timestamp;
        private String _threadName;
        private String _level;
        private String _message;


        public Record(long id)
        {
        }

        public long getId()
        {
            return _id;
        }

        public long getTimestamp()
        {
            return _timestamp;
        }

        public String getThreadName()
        {
            return _threadName;
        }

        public String getLevel()
        {
            return _level;
        }

        public String getMessage()
        {
            return _message;
        }

        public String getLogger()
        {
            return _logger;
        }
    }

    public void closeLogRecorder()
    {
    }

    @Override
    public Iterator<Record> iterator()
    {
        return new RecordIterator(Math.max(_recordId-_bufferSize, 0l));
    }

    private class RecordIterator implements Iterator<Record>
    {
        private long _id;

        public RecordIterator(long currentRecordId)
        {
            _id = currentRecordId;
        }

        @Override
        public boolean hasNext()
        {
            return _id < _recordId;
        }

        @Override
        public Record next()
        {
            Record record = _records[((int) (_id & _mask))];
            while(_id < _recordId-_bufferSize)
            {
                _id = _recordId-_bufferSize;
                record = _records[((int) (_id & _mask))];
            }
            _id++;
            return record;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
