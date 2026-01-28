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

public interface TransportSymbolTexts
{
    String AMQP_ATTACH = "amqp:attach:list";
    String AMQP_BEGIN = "amqp:begin:list";
    String AMQP_CLOSE = "amqp:close:list";
    String AMQP_CONN_ESTABLISHMENT_FAILED = "amqp:connection-establishment-failed";
    String AMQP_CONN_FORCED = "amqp:connection:forced";
    String AMQP_CONN_FRAMING_ERROR = "amqp:connection:framing-error";
    String AMQP_CONN_REDIRECT = "amqp:connection:redirect";
    String AMQP_CONN_SOCKET_ERROR = "amqp:connection:socket-error";
    String AMQP_DETACH = "amqp:detach:list";
    String AMQP_DISPOSITION = "amqp:disposition:list";
    String AMQP_END = "amqp:end:list";
    String AMQP_ERROR = "amqp:error:list";
    String AMQP_ERR_DECODE = "amqp:decode-error";
    String AMQP_ERR_FRAME_SIZE_TOO_SMALL = "amqp:frame-size-too-small";
    String AMQP_ERR_ILLEGAL_STATE = "amqp:illegal-state";
    String AMQP_ERR_INTERNAL = "amqp:internal-error";
    String AMQP_ERR_INVALID_FIELD = "amqp:invalid-field";
    String AMQP_ERR_NOT_ALLOWED = "amqp:not-allowed";
    String AMQP_ERR_NOT_AUTHORIZED = "amqp:unauthorized-access";
    String AMQP_ERR_NOT_FOUND = "amqp:not-found";
    String AMQP_ERR_NOT_IMPLEMENTED = "amqp:not-implemented";
    String AMQP_ERR_PRECONDITION_FAILED = "amqp:precondition-failed";
    String AMQP_ERR_RESOURCE_DELETED = "amqp:resource-deleted";
    String AMQP_ERR_RESOURCE_LIMIT_EXCEEDED = "amqp:resource-limit-exceeded";
    String AMQP_ERR_RESOURCE_LOCKED = "amqp:resource-locked";
    String AMQP_FLOW = "amqp:flow:list";
    String AMQP_LINK_DETACH_FORCED = "amqp:link:detach-forced";
    String AMQP_LINK_MSG_SIZE_EXCEEDED = "amqp:link:message-size-exceeded";
    String AMQP_LINK_REDIRECT = "amqp:link:redirect";
    String AMQP_LINK_STOLEN = "amqp:link:stolen";
    String AMQP_LINK_TRANSFER_LIMIT_EXCEEDED = "amqp:link:transfer-limit-exceeded";
    String AMQP_OPEN = "amqp:open:list";
    String AMQP_SESSION_ERRANT_LINK = "amqp:session:errant-link";
    String AMQP_SESSION_HANDLE_IN_USE = "amqp:session:handle-in-use";
    String AMQP_SESSION_UNATTACHED_HANDLE = "amqp:session:unattached-handle";
    String AMQP_SESSION_WINDOW_VIOLATION = "amqp:session:window-violation";
    String AMQP_TRANSFER = "amqp:transfer:list";
    String CONTAINER_ID = "container-id";
    String FIELD = "field";
    String FILTER = "filter";
    String INVALID_FIELD = "invalid-field";
    String NETWORK_HOST = "network-host";
    String PORT = "port";
    String PRODUCT = "product";
    String SHARED_SUBSCRIPTIONS = "SHARED-SUBS";
    String SOLE_CONNECTION_DETECTION_POLICY = "sole-connection-detection-policy";
    String SOLE_CONNECTION_ENFORCEMENT = "sole-connection-enforcement";
    String SOLE_CONNECTION_ENFORCEMENT_POLICY = "sole-connection-enforcement-policy";
    String SOLE_CONNECTION_FOR_CONTAINER = "sole-connection-for-container";
    String VERSION = "version";
}
