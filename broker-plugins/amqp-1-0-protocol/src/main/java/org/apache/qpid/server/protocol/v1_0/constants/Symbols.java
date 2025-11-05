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

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

/**
 * Utility class to store constant Symbols.
 *
 * Please note that {@link SymbolTexts} contain the string definitions for this class and field names in {@link Symbols}
 * must match the field names in {@link SymbolTexts}, otherwise the annotation processot in broker-codegen module will throw
 * an exception during the compilation
 */
public final class Symbols
{
    private Symbols()
    {

    }

    public static final Symbol AMQP_ACCEPTED = Symbol.valueOf(SymbolTexts.AMQP_ACCEPTED);
    public static final Symbol AMQP_APPLICATION_PROPERTIES = Symbol.valueOf(SymbolTexts.AMQP_APPLICATION_PROPERTIES);
    public static final Symbol AMQP_CONN_ESTABLISHMENT_FAILED = Symbol.valueOf(SymbolTexts.AMQP_CONN_ESTABLISHMENT_FAILED);
    public static final Symbol AMQP_CONN_FORCED = Symbol.valueOf(SymbolTexts.AMQP_CONN_FORCED);
    public static final Symbol AMQP_CONN_FRAMING_ERROR = Symbol.valueOf(SymbolTexts.AMQP_CONN_FRAMING_ERROR);
    public static final Symbol AMQP_CONN_REDIRECT = Symbol.valueOf(SymbolTexts.AMQP_CONN_REDIRECT);
    public static final Symbol AMQP_CONN_SOCKET_ERROR = Symbol.valueOf(SymbolTexts.AMQP_CONN_SOCKET_ERROR);
    public static final Symbol AMQP_DATA = Symbol.valueOf(SymbolTexts.AMQP_DATA);
    public static final Symbol AMQP_DELETE_ON_CLOSE = Symbol.valueOf(SymbolTexts.AMQP_DELETE_ON_CLOSE);
    public static final Symbol AMQP_DELETE_ON_NO_LINKS = Symbol.valueOf(SymbolTexts.AMQP_DELETE_ON_NO_LINKS);
    public static final Symbol AMQP_DELETE_ON_NO_LINKS_OR_MSGS = Symbol.valueOf(SymbolTexts.AMQP_DELETE_ON_NO_LINKS_OR_MSGS);
    public static final Symbol AMQP_DELETE_ON_NO_MSGS = Symbol.valueOf(SymbolTexts.AMQP_DELETE_ON_NO_MSGS);
    public static final Symbol AMQP_DETACH = Symbol.valueOf(SymbolTexts.AMQP_DETACH);
    public static final Symbol AMQP_DELIVERY_ANNOTATIONS = Symbol.valueOf(SymbolTexts.AMQP_DELIVERY_ANNOTATIONS);
    public static final Symbol AMQP_DISPOSITION = Symbol.valueOf(SymbolTexts.AMQP_DISPOSITION);
    public static final Symbol AMQP_DISTRIBUTED_TXN = Symbol.valueOf(SymbolTexts.AMQP_DISTRIBUTED_TXN);
    public static final Symbol AMQP_END = Symbol.valueOf(SymbolTexts.AMQP_END);
    public static final Symbol AMQP_ERROR = Symbol.valueOf(SymbolTexts.AMQP_ERROR);
    public static final Symbol AMQP_ERR_DECODE = Symbol.valueOf(SymbolTexts.AMQP_ERR_DECODE);
    public static final Symbol AMQP_ERR_FRAME_SIZE_TOO_SMALL = Symbol.valueOf(SymbolTexts.AMQP_ERR_FRAME_SIZE_TOO_SMALL);
    public static final Symbol AMQP_ERR_ILLEGAL_STATE = Symbol.valueOf(SymbolTexts.AMQP_ERR_ILLEGAL_STATE);
    public static final Symbol AMQP_ERR_INTERNAL = Symbol.valueOf(SymbolTexts.AMQP_ERR_INTERNAL);
    public static final Symbol AMQP_ERR_INVALID_FIELD = Symbol.valueOf(SymbolTexts.AMQP_ERR_INVALID_FIELD);
    public static final Symbol AMQP_ERR_NOT_ALLOWED = Symbol.valueOf(SymbolTexts.AMQP_ERR_NOT_ALLOWED);
    public static final Symbol AMQP_ERR_NOT_AUTHORIZED = Symbol.valueOf(SymbolTexts.AMQP_ERR_NOT_AUTHORIZED);
    public static final Symbol AMQP_ERR_NOT_FOUND = Symbol.valueOf(SymbolTexts.AMQP_ERR_NOT_FOUND);
    public static final Symbol AMQP_ERR_NOT_IMPLEMENTED = Symbol.valueOf(SymbolTexts.AMQP_ERR_NOT_IMPLEMENTED);
    public static final Symbol AMQP_ERR_PRECONDITION_FAILED = Symbol.valueOf(SymbolTexts.AMQP_ERR_PRECONDITION_FAILED);
    public static final Symbol AMQP_ERR_RESOURCE_DELETED = Symbol.valueOf(SymbolTexts.AMQP_ERR_RESOURCE_DELETED);
    public static final Symbol AMQP_ERR_RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf(SymbolTexts.AMQP_ERR_RESOURCE_LIMIT_EXCEEDED);
    public static final Symbol AMQP_ERR_RESOURCE_LOCKED = Symbol.valueOf(SymbolTexts.AMQP_ERR_RESOURCE_LOCKED);
    public static final Symbol AMQP_FLOW = Symbol.valueOf(SymbolTexts.AMQP_FLOW);
    public static final Symbol AMQP_FOOTER = Symbol.valueOf(SymbolTexts.AMQP_FOOTER);
    public static final Symbol AMQP_HEADER = Symbol.valueOf(SymbolTexts.AMQP_HEADER);
    public static final Symbol AMQP_LINK_DETACH_FORCED = Symbol.valueOf(SymbolTexts.AMQP_LINK_DETACH_FORCED);
    public static final Symbol AMQP_LINK_MSG_SIZE_EXCEEDED = Symbol.valueOf(SymbolTexts.AMQP_LINK_MSG_SIZE_EXCEEDED);
    public static final Symbol AMQP_LINK_REDIRECT = Symbol.valueOf(SymbolTexts.AMQP_LINK_REDIRECT);
    public static final Symbol AMQP_LINK_STOLEN = Symbol.valueOf(SymbolTexts.AMQP_LINK_STOLEN);
    public static final Symbol AMQP_LINK_TRANSFER_LIMIT_EXCEEDED = Symbol.valueOf(SymbolTexts.AMQP_LINK_TRANSFER_LIMIT_EXCEEDED);
    public static final Symbol AMQP_LOCAL_TXN = Symbol.valueOf(SymbolTexts.AMQP_LOCAL_TXN);
    public static final Symbol AMQP_MESSAGE_ANNOTATIONS = Symbol.valueOf(SymbolTexts.AMQP_MESSAGE_ANNOTATIONS);
    public static final Symbol AMQP_MODIFIED = Symbol.valueOf(SymbolTexts.AMQP_MODIFIED);
    public static final Symbol AMQP_MULTI_SESSIONS_PER_TXN = Symbol.valueOf(SymbolTexts.AMQP_MULTI_SESSIONS_PER_TXN);
    public static final Symbol AMQP_MULTI_TXN_PER_SESSION = Symbol.valueOf(SymbolTexts.AMQP_MULTI_TXN_PER_SESSION);
    public static final Symbol AMQP_OPEN = Symbol.valueOf(SymbolTexts.AMQP_OPEN);
    public static final Symbol AMQP_PROMOTABLE_TXN = Symbol.valueOf(SymbolTexts.AMQP_PROMOTABLE_TXN);
    public static final Symbol AMQP_PROPERTIES = Symbol.valueOf(SymbolTexts.AMQP_PROPERTIES);
    public static final Symbol AMQP_RECEIVED = Symbol.valueOf(SymbolTexts.AMQP_RECEIVED);
    public static final Symbol AMQP_REJECTED = Symbol.valueOf(SymbolTexts.AMQP_REJECTED);
    public static final Symbol AMQP_RELEASED = Symbol.valueOf(SymbolTexts.AMQP_RELEASED);
    public static final Symbol AMQP_SASL_CHALLENGE = Symbol.valueOf(SymbolTexts.AMQP_SASL_CHALLENGE);
    public static final Symbol AMQP_SASL_INIT = Symbol.valueOf(SymbolTexts.AMQP_SASL_INIT);
    public static final Symbol AMQP_SASL_MECHANISMS = Symbol.valueOf(SymbolTexts.AMQP_SASL_MECHANISMS);
    public static final Symbol AMQP_SASL_OUTCOME = Symbol.valueOf(SymbolTexts.AMQP_SASL_OUTCOME);
    public static final Symbol AMQP_SASL_RESPONSE = Symbol.valueOf(SymbolTexts.AMQP_SASL_RESPONSE);
    public static final Symbol AMQP_SEQUENCE = Symbol.valueOf(SymbolTexts.AMQP_SEQUENCE);
    public static final Symbol AMQP_SESSION_ERRANT_LINK = Symbol.valueOf(SymbolTexts.AMQP_SESSION_ERRANT_LINK);
    public static final Symbol AMQP_SESSION_HANDLE_IN_USE = Symbol.valueOf(SymbolTexts.AMQP_SESSION_HANDLE_IN_USE);
    public static final Symbol AMQP_SESSION_UNATTACHED_HANDLE = Symbol.valueOf(SymbolTexts.AMQP_SESSION_UNATTACHED_HANDLE);
    public static final Symbol AMQP_SESSION_WINDOW_VIOLATION = Symbol.valueOf(SymbolTexts.AMQP_SESSION_WINDOW_VIOLATION);
    public static final Symbol AMQP_SOURCE = Symbol.valueOf(SymbolTexts.AMQP_SOURCE);
    public static final Symbol AMQP_TARGET = Symbol.valueOf(SymbolTexts.AMQP_TARGET);
    public static final Symbol AMQP_TRANSFER = Symbol.valueOf(SymbolTexts.AMQP_TRANSFER);
    public static final Symbol AMQP_TXN_COORDINATOR = Symbol.valueOf(SymbolTexts.AMQP_TXN_COORDINATOR);
    public static final Symbol AMQP_TXN_DECLARE = Symbol.valueOf(SymbolTexts.AMQP_TXN_DECLARE);
    public static final Symbol AMQP_TXN_DECLARED = Symbol.valueOf(SymbolTexts.AMQP_TXN_DECLARED);
    public static final Symbol AMQP_TXN_DISCHARGE = Symbol.valueOf(SymbolTexts.AMQP_TXN_DISCHARGE);
    public static final Symbol AMQP_TXN_ROLLBACK = Symbol.valueOf(SymbolTexts.AMQP_TXN_ROLLBACK);
    public static final Symbol AMQP_TXN_STATE = Symbol.valueOf(SymbolTexts.AMQP_TXN_STATE);
    public static final Symbol AMQP_TXN_TIMEOUT = Symbol.valueOf(SymbolTexts.AMQP_TXN_TIMEOUT);
    public static final Symbol AMQP_TXN_UNKNOWN_ID = Symbol.valueOf(SymbolTexts.AMQP_TXN_UNKNOWN_ID);
    public static final Symbol AMQP_VALUE = Symbol.valueOf(SymbolTexts.AMQP_VALUE);
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf(SymbolTexts.ANONYMOUS_RELAY);
    public static final Symbol ANNOTATION_KEY = Symbol.valueOf(SymbolTexts.ANNOTATION_KEY);
    public static final Symbol APACHE_LEGACY_DIRECT_BINDING = Symbol.valueOf(SymbolTexts.APACHE_LEGACY_DIRECT_BINDING);
    public static final Symbol APACHE_LEGACY_NO_LOCAL_FILTER = Symbol.valueOf(SymbolTexts.APACHE_LEGACY_NO_LOCAL_FILTER);
    public static final Symbol APACHE_LEGACY_SELECTOR_FILTER = Symbol.valueOf(SymbolTexts.APACHE_LEGACY_SELECTOR_FILTER);
    public static final Symbol APACHE_LEGACY_TOPIC_BINDING = Symbol.valueOf(SymbolTexts.APACHE_LEGACY_TOPIC_BINDING);
    public static final Symbol APACHE_NO_LOCAL_FILTER = Symbol.valueOf(SymbolTexts.APACHE_NO_LOCAL_FILTER);
    public static final Symbol APACHE_SELECTOR_FILTER = Symbol.valueOf(SymbolTexts.APACHE_SELECTOR_FILTER);
    public static final Symbol APP_OCTET_STREAM = Symbol.valueOf(SymbolTexts.APP_OCTET_STREAM);
    public static final Symbol APP_X_JAVA_SERIALIZED_OBJ = Symbol.valueOf(SymbolTexts.APP_X_JAVA_SERIALIZED_OBJ);
    public static final Symbol CONNECTION_CLOSE = Symbol.valueOf(SymbolTexts.CONNECTION_CLOSE);
    public static final Symbol CONTAINER_ID = Symbol.valueOf(SymbolTexts.CONTAINER_ID);
    public static final Symbol COPY = Symbol.valueOf(SymbolTexts.COPY);
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf(SymbolTexts.DELAYED_DELIVERY);
    public static final Symbol DELIVERY_TAG = Symbol.valueOf(SymbolTexts.DELIVERY_TAG);
    public static final Symbol DELIVERY_TIME = Symbol.valueOf(SymbolTexts.DELIVERY_TIME);
    public static final Symbol DISCARD_UNROUTABLE = Symbol.valueOf(SymbolTexts.DISCARD_UNROUTABLE);
    public static final Symbol FIELD = Symbol.valueOf(SymbolTexts.FIELD);
    public static final Symbol FILTER = Symbol.valueOf(SymbolTexts.FILTER);
    public static final Symbol GLOBAL_CAPABILITY = Symbol.getSymbol(SymbolTexts.GLOBAL_CAPABILITY);
    public static final Symbol INVALID_FIELD = Symbol.valueOf(SymbolTexts.INVALID_FIELD);
    public static final Symbol LIFETIME_POLICY = Symbol.valueOf(SymbolTexts.LIFETIME_POLICY);
    public static final Symbol LINK_DETACH = Symbol.valueOf(SymbolTexts.LINK_DETACH);
    public static final Symbol MOVE = Symbol.valueOf(SymbolTexts.MOVE);
    public static final Symbol NETWORK_HOST = Symbol.valueOf(SymbolTexts.NETWORK_HOST);
    public static final Symbol NEVER = Symbol.valueOf(SymbolTexts.NEVER);
    public static final Symbol NOT_VALID_BEFORE = Symbol.valueOf(SymbolTexts.NOT_VALID_BEFORE);
    public static final Symbol PRIORITY = Symbol.valueOf(SymbolTexts.PRIORITY);
    public static final Symbol PORT = Symbol.valueOf(SymbolTexts.PORT);
    public static final Symbol PRODUCT = Symbol.valueOf(SymbolTexts.PRODUCT);
    public static final Symbol REJECT_UNROUTABLE = Symbol.valueOf(SymbolTexts.REJECT_UNROUTABLE);
    public static final Symbol SHARED_CAPABILITY = Symbol.getSymbol(SymbolTexts.SHARED_CAPABILITY);
    public static final Symbol SESSION_END = Symbol.valueOf(SymbolTexts.SESSION_END);
    public static final Symbol SHARED_SUBSCRIPTIONS = Symbol.valueOf(SymbolTexts.SHARED_SUBSCRIPTIONS);
    public static final Symbol SOLE_CONNECTION_ENFORCEMENT = Symbol.valueOf(SymbolTexts.SOLE_CONNECTION_ENFORCEMENT);
    public static final Symbol SOLE_CONNECTION_ENFORCEMENT_POLICY = Symbol.valueOf(SymbolTexts.SOLE_CONNECTION_ENFORCEMENT_POLICY);
    public static final Symbol SOLE_CONNECTION_DETECTION_POLICY = Symbol.valueOf(SymbolTexts.SOLE_CONNECTION_DETECTION_POLICY);
    public static final Symbol SOLE_CONNECTION_FOR_CONTAINER = Symbol.valueOf(SymbolTexts.SOLE_CONNECTION_FOR_CONTAINER);
    public static final Symbol SUPPORTED_DIST_MODES = Symbol.valueOf(SymbolTexts.SUPPORTED_DIST_MODES);
    public static final Symbol TEMPORARY_QUEUE = Symbol.valueOf(SymbolTexts.TEMPORARY_QUEUE);
    public static final Symbol TEMPORARY_TOPIC = Symbol.valueOf(SymbolTexts.TEMPORARY_TOPIC);
    public static final Symbol TOPIC = Symbol.valueOf(SymbolTexts.TOPIC);
    public static final Symbol TXN_ID = Symbol.valueOf(SymbolTexts.TXN_ID);
    public static final Symbol VERSION = Symbol.valueOf(SymbolTexts.VERSION);
}
