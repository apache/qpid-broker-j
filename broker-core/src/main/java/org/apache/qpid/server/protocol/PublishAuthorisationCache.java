/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.protocol;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.transport.AbstractAMQPConnection;

public class PublishAuthorisationCache
{
    private final SecurityToken _token;

    private final long _publishAuthCacheTimeout;
    private final int _publishAuthCacheSize;
    private final LinkedHashMap<PublishAuthKey, Long> _publishAuthCache =
            new LinkedHashMap<PublishAuthKey, Long>()
            {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<PublishAuthKey, Long> eldest)
                {
                    return size() > _publishAuthCacheSize;
                }
            };

    public PublishAuthorisationCache(final SecurityToken token,
                                     final long publishAuthCacheTimeout,
                                     final int publishAuthCacheSize)
    {
        _token = token;
        _publishAuthCacheTimeout = publishAuthCacheTimeout;
        _publishAuthCacheSize = publishAuthCacheSize;
    }

    private final class PublishAuthKey
    {
        private final MessageDestination _messageDestination;
        private final String _routingKey;
        private final boolean _immediate;
        private final int _hashCode;

        public PublishAuthKey(final MessageDestination messageDestination,
                              final String routingKey,
                              final boolean immediate)
        {
            _messageDestination = messageDestination;
            _routingKey = routingKey;
            _immediate = immediate;
            _hashCode = Objects.hash(_messageDestination, _routingKey, _immediate);
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final PublishAuthKey that = (PublishAuthKey) o;
            return _hashCode == that._hashCode
                   && _immediate == that._immediate
                   && Objects.equals(_messageDestination, that._messageDestination)
                   && Objects.equals(_routingKey, that._routingKey);
        }

        @Override
        public int hashCode()
        {
            return _hashCode;
        }
    }

    public void authorisePublish(MessageDestination destination, String routingKey, boolean isImmediate, long currentTime)
    {
        PublishAuthKey key = new PublishAuthKey(destination, routingKey, isImmediate);
        Long expiration = _publishAuthCache.get(key);

        if(expiration == null || expiration < currentTime)
        {
            destination.authorisePublish(_token, AbstractAMQPConnection.PUBLISH_ACTION_MAP_CREATOR.createMap(routingKey, isImmediate));
            _publishAuthCache.put(key, currentTime + _publishAuthCacheTimeout);
        }
    }

}
