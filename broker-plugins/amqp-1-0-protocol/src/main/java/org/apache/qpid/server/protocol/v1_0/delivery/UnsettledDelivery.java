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
package org.apache.qpid.server.protocol.v1_0.delivery;

import java.util.Objects;

import org.apache.qpid.server.protocol.v1_0.LinkEndpoint;
import org.apache.qpid.server.protocol.v1_0.type.Binary;

public class UnsettledDelivery
{
    private final Binary _deliveryTag;
    private final LinkEndpoint<?,?> _linkEndpoint;

    public UnsettledDelivery(final Binary deliveryTag, final LinkEndpoint<?, ?> linkEndpoint)
    {
        _deliveryTag = deliveryTag;
        _linkEndpoint = linkEndpoint;
    }

    public Binary getDeliveryTag()
    {
        return _deliveryTag;
    }

    public LinkEndpoint<?, ?> getLinkEndpoint()
    {
        return _linkEndpoint;
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
        final UnsettledDelivery that = (UnsettledDelivery) o;
        return Objects.equals(_deliveryTag, that._deliveryTag) &&
               Objects.equals(_linkEndpoint, that._linkEndpoint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_deliveryTag, _linkEndpoint);
    }
}
