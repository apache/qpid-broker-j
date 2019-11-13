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
package org.apache.qpid.server.security;

import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.virtualhost.AbstractSystemMessageSource;

public class TrustStoreMessageSource extends AbstractSystemMessageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TrustStoreMessageSource.class);

    private final TrustStore<?> _trustStore;
    private final AtomicReference<Set<Certificate>> _certCache = new AtomicReference<>();
    private final VirtualHost<?> _virtualHost;
    private final AbstractConfigurationChangeListener _trustStoreListener;


    public TrustStoreMessageSource(final TrustStore<?> trustStore, final VirtualHost<?> virtualHost)
    {
        super(getSourceNameFromTrustStore(trustStore), virtualHost);
        _virtualHost = virtualHost;
        _trustStore = trustStore;
        _trustStoreListener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
            {
                if (newState == State.ACTIVE)
                {
                    updateCertCache();
                }
            }

            @Override
            public void attributeSet(final ConfiguredObject<?> object,
                                     final String attributeName,
                                     final Object oldAttributeValue,
                                     final Object newAttributeValue)
            {
                updateCertCache();
            }
        };
        _trustStore.addChangeListener(_trustStoreListener);
        if(_trustStore.getState() == State.ACTIVE)
        {
            updateCertCache();
        }
    }

    @Override
    public <T extends ConsumerTarget<T>> Consumer<T> addConsumer(final T target,
                                final FilterManager filters,
                                final Class<? extends ServerMessage> messageClass,
                                final String consumerName,
                                final EnumSet<ConsumerOption> options, final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {
        final Consumer<T> consumer = super.addConsumer(target, filters, messageClass, consumerName, options, priority);
        consumer.send(createMessage());
        target.noMessagesAvailable();
        return consumer;
    }

    @Override
    public void close()
    {
        _trustStore.removeChangeListener(_trustStoreListener);
    }

    private void updateCertCache()
    {
        _certCache.set(populateCertCache());
        if(!getConsumers().isEmpty())
        {
            sendMessageToConsumers();
        }
    }

    private void sendMessageToConsumers()
    {
        InternalMessage message = createMessage();

        for(Consumer c : new ArrayList<>(getConsumers()))
        {
            c.send(message);
        }

    }

    private InternalMessage createMessage()
    {
        List<Object> messageList = new ArrayList<>();
        for (Certificate cert : _certCache.get())
        {
            try
            {
                messageList.add(cert.getEncoded());
            }
            catch (CertificateEncodingException e)
            {
                LOGGER.error("Could not encode certificate of type " + cert.getType(), e);
            }
        }
        InternalMessageHeader header = new InternalMessageHeader(Collections.<String,Object>emptyMap(),
                                                                 null, 0L, null, null, UUID.randomUUID().toString(),
                                                                 null, null, (byte)4, System.currentTimeMillis(),
                                                                 0L, null, null, System.currentTimeMillis());
        return InternalMessage.createListMessage(_virtualHost.getMessageStore(), header, messageList);
    }

    private Set<Certificate> populateCertCache()
    {
        try
        {
            Set<Certificate> certCache = new HashSet<>();
            Collections.addAll(certCache, _trustStore.getCertificates());
            return certCache;
        }
        catch (GeneralSecurityException e)
        {
            LOGGER.error("Cannot read trust managers from truststore " + _trustStore.getName(), e);
            return Collections.emptySet();
        }
    }


    public static String getSourceNameFromTrustStore(final TrustStore<?> trustStore)
    {
        return "$certificates/" + trustStore.getName();
    }

}
