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
 *
 */

package org.apache.qpid.server.protocol.v1_0.constants;

import org.apache.qpid.server.protocol.v1_0.constants.amqp.capabilities.CapabilitiesSymbols;
import org.apache.qpid.server.protocol.v1_0.constants.amqp.messaging.MessagingSymbols;
import org.apache.qpid.server.protocol.v1_0.constants.amqp.security.SaslSymbols;
import org.apache.qpid.server.protocol.v1_0.constants.amqp.transaction.TransactionSymbols;
import org.apache.qpid.server.protocol.v1_0.constants.amqp.transport.TransportSymbols;
import org.apache.qpid.server.protocol.v1_0.constants.qpid.QpidSymbols;

/**
 * Aggregates all AMQP and Qpid-specific Symbol constants.
 * Field names must match those in {@link SymbolTexts} for code generation mapping.
 */
public interface Symbols extends TransportSymbols, MessagingSymbols, TransactionSymbols, SaslSymbols, CapabilitiesSymbols, QpidSymbols
{
}
