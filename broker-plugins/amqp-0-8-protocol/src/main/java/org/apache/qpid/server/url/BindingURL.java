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
package org.apache.qpid.server.url;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/*
    Binding URL format:
    <exch_class>://<exch_name>/[<destination>]/[<queue>]?<option>='<value>'[,<option>='<value>']*
*/
public interface BindingURL
{
    String OPTION_EXCLUSIVE = "exclusive";
    String OPTION_AUTODELETE = "autodelete";
    String OPTION_DURABLE = "durable";
    String OPTION_BROWSE = "browse";
    String OPTION_ROUTING_KEY = "routingkey";
    String OPTION_BINDING_KEY = "bindingkey";
    String OPTION_EXCHANGE_AUTODELETE = "exchangeautodelete";
    String OPTION_EXCHANGE_DURABLE = "exchangedurable";
    String OPTION_EXCHANGE_INTERNAL = "exchangeinternal";
    String OPTION_SEND_ENCRYPTED = "sendencrypted";
    String OPTION_ENCRYPTED_RECIPIENTS = "encryptedrecipients";
    String OPTION_DELIVERY_DELAY = "deliveryDelay";
    String OPTION_LOCAL_ADDRESS = "localAddress";


    /**
     * This option is only applicable for 0-8/0-9/0-9-1 protocols connection
     * <p>
     * It tells the client to delegate the requeue/DLQ decision to the
     * server .If this option is not specified, the messages won't be moved to
     * the DLQ (or dropped) when delivery count exceeds the maximum.
     */
    String OPTION_REJECT_BEHAVIOUR = "rejectbehaviour";

    Set<String> NON_CONSUMER_OPTIONS =
            Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(OPTION_EXCLUSIVE,
                                                                          OPTION_AUTODELETE,
                                                                          OPTION_DURABLE,
                                                                          OPTION_BROWSE,
                                                                          OPTION_ROUTING_KEY,
                                                                          OPTION_BINDING_KEY,
                                                                          OPTION_EXCHANGE_AUTODELETE,
                                                                          OPTION_EXCHANGE_DURABLE,
                                                                          OPTION_EXCHANGE_DURABLE,
                                                                          OPTION_REJECT_BEHAVIOUR,
                                                                          OPTION_SEND_ENCRYPTED,
                                                                          OPTION_ENCRYPTED_RECIPIENTS,
                                                                          OPTION_DELIVERY_DELAY,
                                                                          OPTION_LOCAL_ADDRESS)));


    String getURL();

    String getExchangeClass();

    String getExchangeName();

    String getDestinationName();

    String getQueueName();

    String getOption(String key);

    Map<String,Object> getConsumerOptions();


    boolean containsOption(String key);

    String getRoutingKey();

    String[] getBindingKeys();

    @Override
    String toString();
}
