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

package org.apache.qpid.server.security;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObjectFactory;

@SuppressWarnings({"rawtypes", "unchecked"})
public class KeyStoreTestHelper
{
    public static void checkExceptionThrownDuringKeyStoreCreation(final ConfiguredObjectFactory factory,
                                                                  final Broker<?> broker,
                                                                  final Class keystoreClass,
                                                                  final Map<String, Object> attributes,
                                                                  final String expectedExceptionMessage)
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> factory.create(keystoreClass, attributes, broker),
                "Exception not thrown");
        final String message = thrown.getMessage();
        assertTrue(message.contains(expectedExceptionMessage), "Exception text not as expected:" + message);
    }
}
