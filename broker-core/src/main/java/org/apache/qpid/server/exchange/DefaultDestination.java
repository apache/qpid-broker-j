/*
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
 */
package org.apache.qpid.server.exchange;

import java.security.AccessControlException;
import java.util.Map;

import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class DefaultDestination implements MessageDestination, PermissionedObject
{

    private static final Operation PUBLISH_ACTION = Operation.PERFORM_ACTION("publish");
    private final AccessControl _accessControl;
    private QueueManagingVirtualHost<?> _virtualHost;

    public DefaultDestination(QueueManagingVirtualHost<?> virtualHost, final AccessControl accessControl)
    {
        _virtualHost =  virtualHost;
        _accessControl = accessControl;
    }

    @Override
    public Class<? extends ConfiguredObject> getCategoryClass()
    {
        return Exchange.class;
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _virtualHost;
    }


    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {

        if(_accessControl != null)
        {
            Result result = _accessControl.authorise(token, PUBLISH_ACTION, this, arguments);
            if (result == Result.DEFER)
            {
                result = _accessControl.getDefault();
            }

            if (result == Result.DENIED)
            {
                throw new AccessControlException("Access denied to publish to default exchange with arguments: " + arguments);
            }
        }
    }

    @Override
    public String getName()
    {
        return ExchangeDefaults.DEFAULT_EXCHANGE_NAME;
    }


    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(M message,
                                                                                               String routingAddress,
                                                                                               InstanceProperties instanceProperties)
    {
        RoutingResult<M> result = new RoutingResult<>(message);

        DestinationAddress destinationAddress = new DestinationAddress(_virtualHost, routingAddress, true);
        MessageDestination messageDestination = destinationAddress.getMessageDestination();
        if (messageDestination != null)
        {
            result.add(messageDestination.route(message,destinationAddress.getRoutingKey(), instanceProperties));
        }
        return result;
    }

    @Override
    public boolean isDurable()
    {
        return true;
    }

    @Override
    public void linkAdded(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public void linkRemoved(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public MessageDestination getAlternateBindingDestination()
    {
        return null;
    }

    @Override
    public void removeReference(final DestinationReferrer destinationReferrer)
    {
    }

    @Override
    public void addReference(final DestinationReferrer destinationReferrer)
    {
    }
}
