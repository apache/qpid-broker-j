/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.server.connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.common.ServerPropertyNames;
import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.transport.AMQPConnection;

@PluggableService
public class StandardConnectionPropertyEnricher implements ConnectionPropertyEnricher
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardConnectionPropertyEnricher.class);

    @Override
    public Map<String, Object> addConnectionProperties(final AMQPConnection<?> connection,
                                                       final Map<String, Object> existingProperties)
    {
        Map<String,Object> modifiedProperties = new LinkedHashMap<>(existingProperties);

        Broker<?> broker = connection.getBroker();

        modifiedProperties.put(ServerPropertyNames.PRODUCT, CommonProperties.getProductName());
        modifiedProperties.put(ServerPropertyNames.VERSION, CommonProperties.getReleaseVersion());
        modifiedProperties.put(ServerPropertyNames.QPID_BUILD, CommonProperties.getBuildVersion());
        modifiedProperties.put(ServerPropertyNames.QPID_INSTANCE_NAME, broker.getName());
        modifiedProperties.put(ConnectionStartProperties.QPID_VIRTUALHOST_PROPERTIES_SUPPORTED,
                               String.valueOf(broker.isVirtualHostPropertiesNodeEnabled()));

        modifiedProperties.put(ConnectionStartProperties.QPID_MESSAGE_COMPRESSION_SUPPORTED,
                               String.valueOf(broker.isMessageCompressionEnabled()));
        modifiedProperties.put(ConnectionStartProperties.QPID_QUEUE_LIFETIME_SUPPORTED, Boolean.TRUE.toString());


        switch (connection.getProtocol())
        {
            case AMQP_0_8:
            case AMQP_0_9:
            case AMQP_0_9_1:
                modifiedProperties.put(ConnectionStartProperties.QPID_CONFIRMED_PUBLISH_SUPPORTED,
                                       Boolean.TRUE.toString());
                modifiedProperties.put(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE,
                                       String.valueOf(connection.getPort().getCloseWhenNoRoute()));

                break;
            case AMQP_0_10:
                // Federation tag is used by the client to identify the broker instance
                modifiedProperties.put(ServerPropertyNames.FEDERATION_TAG, broker.getId().toString());
                final List<String> features = getFeatures(broker);
                if (features.size() > 0)
                {
                    modifiedProperties.put(ServerPropertyNames.QPID_FEATURES, features);
                }
                break;
            case AMQP_1_0:
                // message compression is not supported in 1.0
                modifiedProperties.remove(ConnectionStartProperties.QPID_MESSAGE_COMPRESSION_SUPPORTED);
                // this property is only meaningful for queue declare operations in 0-x
                modifiedProperties.remove(ConnectionStartProperties.QPID_QUEUE_LIFETIME_SUPPORTED);

                break;
            default:
                LOG.info("Unexpected protocol: " + connection.getProtocol());
                break;
        }

        return Collections.unmodifiableMap(modifiedProperties);
    }

    private static List<String> getFeatures(Broker<?> broker)
    {
        String brokerDisabledFeatures = System.getProperty(Broker.PROPERTY_DISABLED_FEATURES);
        final List<String> features = new ArrayList<String>();
        if (brokerDisabledFeatures == null || !brokerDisabledFeatures.contains(ServerPropertyNames.FEATURE_QPID_JMS_SELECTOR))
        {
            features.add(ServerPropertyNames.FEATURE_QPID_JMS_SELECTOR);
        }

        return Collections.unmodifiableList(features);
    }


    @Override
    public String getType()
    {
        return "STANDARD";
    }
}
