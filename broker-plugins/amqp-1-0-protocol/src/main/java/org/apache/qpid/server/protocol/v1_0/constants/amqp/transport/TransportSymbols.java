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

package org.apache.qpid.server.protocol.v1_0.constants.amqp.transport;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public interface TransportSymbols
{
    Symbol AMQP_ATTACH = Symbol.valueOf(TransportSymbolTexts.AMQP_ATTACH);
    Symbol AMQP_BEGIN = Symbol.valueOf(TransportSymbolTexts.AMQP_BEGIN);
    Symbol AMQP_CLOSE = Symbol.valueOf(TransportSymbolTexts.AMQP_CLOSE);
    Symbol AMQP_CONN_ESTABLISHMENT_FAILED = Symbol.valueOf(TransportSymbolTexts.AMQP_CONN_ESTABLISHMENT_FAILED);
    Symbol AMQP_CONN_FORCED = Symbol.valueOf(TransportSymbolTexts.AMQP_CONN_FORCED);
    Symbol AMQP_CONN_FRAMING_ERROR = Symbol.valueOf(TransportSymbolTexts.AMQP_CONN_FRAMING_ERROR);
    Symbol AMQP_CONN_REDIRECT = Symbol.valueOf(TransportSymbolTexts.AMQP_CONN_REDIRECT);
    Symbol AMQP_CONN_SOCKET_ERROR = Symbol.valueOf(TransportSymbolTexts.AMQP_CONN_SOCKET_ERROR);
    Symbol AMQP_DETACH = Symbol.valueOf(TransportSymbolTexts.AMQP_DETACH);
    Symbol AMQP_DISPOSITION = Symbol.valueOf(TransportSymbolTexts.AMQP_DISPOSITION);
    Symbol AMQP_END = Symbol.valueOf(TransportSymbolTexts.AMQP_END);
    Symbol AMQP_ERROR = Symbol.valueOf(TransportSymbolTexts.AMQP_ERROR);
    Symbol AMQP_ERR_DECODE = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_DECODE);
    Symbol AMQP_ERR_FRAME_SIZE_TOO_SMALL = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_FRAME_SIZE_TOO_SMALL);
    Symbol AMQP_ERR_ILLEGAL_STATE = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_ILLEGAL_STATE);
    Symbol AMQP_ERR_INTERNAL = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_INTERNAL);
    Symbol AMQP_ERR_INVALID_FIELD = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_INVALID_FIELD);
    Symbol AMQP_ERR_NOT_ALLOWED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_NOT_ALLOWED);
    Symbol AMQP_ERR_NOT_AUTHORIZED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_NOT_AUTHORIZED);
    Symbol AMQP_ERR_NOT_FOUND = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_NOT_FOUND);
    Symbol AMQP_ERR_NOT_IMPLEMENTED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_NOT_IMPLEMENTED);
    Symbol AMQP_ERR_PRECONDITION_FAILED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_PRECONDITION_FAILED);
    Symbol AMQP_ERR_RESOURCE_DELETED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_RESOURCE_DELETED);
    Symbol AMQP_ERR_RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_RESOURCE_LIMIT_EXCEEDED);
    Symbol AMQP_ERR_RESOURCE_LOCKED = Symbol.valueOf(TransportSymbolTexts.AMQP_ERR_RESOURCE_LOCKED);
    Symbol AMQP_FLOW = Symbol.valueOf(TransportSymbolTexts.AMQP_FLOW);
    Symbol AMQP_LINK_DETACH_FORCED = Symbol.valueOf(TransportSymbolTexts.AMQP_LINK_DETACH_FORCED);
    Symbol AMQP_LINK_MSG_SIZE_EXCEEDED = Symbol.valueOf(TransportSymbolTexts.AMQP_LINK_MSG_SIZE_EXCEEDED);
    Symbol AMQP_LINK_REDIRECT = Symbol.valueOf(TransportSymbolTexts.AMQP_LINK_REDIRECT);
    Symbol AMQP_LINK_STOLEN = Symbol.valueOf(TransportSymbolTexts.AMQP_LINK_STOLEN);
    Symbol AMQP_LINK_TRANSFER_LIMIT_EXCEEDED = Symbol.valueOf(TransportSymbolTexts.AMQP_LINK_TRANSFER_LIMIT_EXCEEDED);
    Symbol AMQP_OPEN = Symbol.valueOf(TransportSymbolTexts.AMQP_OPEN);
    Symbol AMQP_SESSION_ERRANT_LINK = Symbol.valueOf(TransportSymbolTexts.AMQP_SESSION_ERRANT_LINK);
    Symbol AMQP_SESSION_HANDLE_IN_USE = Symbol.valueOf(TransportSymbolTexts.AMQP_SESSION_HANDLE_IN_USE);
    Symbol AMQP_SESSION_UNATTACHED_HANDLE = Symbol.valueOf(TransportSymbolTexts.AMQP_SESSION_UNATTACHED_HANDLE);
    Symbol AMQP_SESSION_WINDOW_VIOLATION = Symbol.valueOf(TransportSymbolTexts.AMQP_SESSION_WINDOW_VIOLATION);
    Symbol AMQP_TRANSFER = Symbol.valueOf(TransportSymbolTexts.AMQP_TRANSFER);
    Symbol CONTAINER_ID = Symbol.valueOf(TransportSymbolTexts.CONTAINER_ID);
    Symbol FIELD = Symbol.valueOf(TransportSymbolTexts.FIELD);
    Symbol FILTER = Symbol.valueOf(TransportSymbolTexts.FILTER);
    Symbol INVALID_FIELD = Symbol.valueOf(TransportSymbolTexts.INVALID_FIELD);
    Symbol NETWORK_HOST = Symbol.valueOf(TransportSymbolTexts.NETWORK_HOST);
    Symbol PORT = Symbol.valueOf(TransportSymbolTexts.PORT);
    Symbol PRODUCT = Symbol.valueOf(TransportSymbolTexts.PRODUCT);
    Symbol SHARED_SUBSCRIPTIONS = Symbol.valueOf(TransportSymbolTexts.SHARED_SUBSCRIPTIONS);
    Symbol SOLE_CONNECTION_DETECTION_POLICY = Symbol.valueOf(TransportSymbolTexts.SOLE_CONNECTION_DETECTION_POLICY);
    Symbol SOLE_CONNECTION_ENFORCEMENT = Symbol.valueOf(TransportSymbolTexts.SOLE_CONNECTION_ENFORCEMENT);
    Symbol SOLE_CONNECTION_ENFORCEMENT_POLICY = Symbol.valueOf(TransportSymbolTexts.SOLE_CONNECTION_ENFORCEMENT_POLICY);
    Symbol SOLE_CONNECTION_FOR_CONTAINER = Symbol.valueOf(TransportSymbolTexts.SOLE_CONNECTION_FOR_CONTAINER);
    Symbol VERSION = Symbol.valueOf(TransportSymbolTexts.VERSION);
}
