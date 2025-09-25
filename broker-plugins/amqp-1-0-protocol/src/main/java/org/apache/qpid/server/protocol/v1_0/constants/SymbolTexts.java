/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"; you may not use this file except in compliance
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
 * Utility class to store constant {@link Symbol} texts.
 *
 * Please note that this class contain the string definitions for {@link Symbols} class and field names in
 * {@link Symbols} must match the field names in {@link SymbolTexts}, otherwise the annotation processot in broker-codegen
 * module will throw an exception during the compilation
 */
public final class SymbolTexts
{
    private SymbolTexts()
    {

    }

    public static final String AMQP_ACCEPTED = "amqp:accepted:list";
    public static final String AMQP_APPLICATION_PROPERTIES = "amqp:application-properties:map";
    public static final String AMQP_ATTACH = "amqp:attach:list";
    public static final String AMQP_BEGIN = "amqp:begin:list";
    public static final String AMQP_CLOSE = "amqp:close:list";
    public static final String AMQP_CONN_ESTABLISHMENT_FAILED = "amqp:connection-establishment-failed";
    public static final String AMQP_CONN_FORCED = "amqp:connection:forced";
    public static final String AMQP_CONN_FRAMING_ERROR = "amqp:connection:framing-error";
    public static final String AMQP_CONN_REDIRECT = "amqp:connection:redirect";
    public static final String AMQP_CONN_SOCKET_ERROR = "amqp:connection:socket-error";
    public static final String AMQP_DATA = "amqp:data:binary";
    public static final String AMQP_DELETE_ON_CLOSE = "amqp:delete-on-close:list";
    public static final String AMQP_DELETE_ON_NO_LINKS = "amqp:delete-on-no-links:list";
    public static final String AMQP_DELETE_ON_NO_LINKS_OR_MSGS = "amqp:delete-on-no-links-or-messages:list";
    public static final String AMQP_DELETE_ON_NO_MSGS = "amqp:delete-on-no-messages:list";
    public static final String AMQP_DETACH = "amqp:detach:list";
    public static final String AMQP_DELIVERY_ANNOTATIONS = "amqp:delivery-annotations:map";
    public static final String AMQP_DISPOSITION = "amqp:disposition:list";
    public static final String AMQP_DISTRIBUTED_TXN = "amqp:distributed-transactions";
    public static final String AMQP_END = "amqp:end:list";
    public static final String AMQP_ERROR = "amqp:error:list";
    public static final String AMQP_ERR_DECODE = "amqp:decode-error";
    public static final String AMQP_ERR_FRAME_SIZE_TOO_SMALL = "amqp:frame-size-too-small";
    public static final String AMQP_ERR_ILLEGAL_STATE = "amqp:illegal-state";
    public static final String AMQP_ERR_INTERNAL = "amqp:internal-error";
    public static final String AMQP_ERR_INVALID_FIELD = "amqp:invalid-field";
    public static final String AMQP_ERR_NOT_ALLOWED = "amqp:not-allowed";
    public static final String AMQP_ERR_NOT_AUTHORIZED = "amqp:unauthorized-access";
    public static final String AMQP_ERR_NOT_FOUND = "amqp:not-found";
    public static final String AMQP_ERR_NOT_IMPLEMENTED = "amqp:not-implemented";
    public static final String AMQP_ERR_PRECONDITION_FAILED = "amqp:precondition-failed";
    public static final String AMQP_ERR_RESOURCE_DELETED = "amqp:resource-deleted";
    public static final String AMQP_ERR_RESOURCE_LIMIT_EXCEEDED = "amqp:resource-limit-exceeded";
    public static final String AMQP_ERR_RESOURCE_LOCKED = "amqp:resource-locked";
    public static final String AMQP_FLOW = "amqp:flow:list";
    public static final String AMQP_FOOTER = "amqp:footer:map";
    public static final String AMQP_HEADER = "amqp:header:list";
    public static final String AMQP_LINK_DETACH_FORCED = "amqp:link:detach-forced";
    public static final String AMQP_LINK_MSG_SIZE_EXCEEDED = "amqp:link:message-size-exceeded";
    public static final String AMQP_LINK_REDIRECT = "amqp:link:redirect";
    public static final String AMQP_LINK_STOLEN = "amqp:link:stolen";
    public static final String AMQP_LINK_TRANSFER_LIMIT_EXCEEDED = "amqp:link:transfer-limit-exceeded";
    public static final String AMQP_LOCAL_TXN = "amqp:local-transactions";
    public static final String AMQP_MESSAGE_ANNOTATIONS = "amqp:message-annotations:map";
    public static final String AMQP_MODIFIED = "amqp:modified:list";
    public static final String AMQP_MULTI_SESSIONS_PER_TXN = "amqp:multi-ssns-per-txn";
    public static final String AMQP_MULTI_TXN_PER_SESSION = "amqp:multi-txns-per-ssn";
    public static final String AMQP_OPEN = "amqp:open:list";
    public static final String AMQP_PROMOTABLE_TXN = "amqp:promotable-transactions";
    public static final String AMQP_PROPERTIES = "amqp:properties:list";
    public static final String AMQP_RECEIVED = "amqp:received:list";
    public static final String AMQP_REJECTED = "amqp:rejected:list";
    public static final String AMQP_RELEASED = "amqp:released:list";
    public static final String AMQP_SASL_CHALLENGE = "amqp:sasl-challenge:list";
    public static final String AMQP_SASL_INIT = "amqp:sasl-init:list";
    public static final String AMQP_SASL_MECHANISMS = "amqp:sasl-mechanisms:list";
    public static final String AMQP_SASL_OUTCOME = "amqp:sasl-outcome:list";
    public static final String AMQP_SASL_RESPONSE = "amqp:sasl-response:list";
    public static final String AMQP_SEQUENCE = "amqp:amqp-sequence:list";
    public static final String AMQP_SESSION_ERRANT_LINK = "amqp:session:errant-link";
    public static final String AMQP_SESSION_HANDLE_IN_USE = "amqp:session:handle-in-use";
    public static final String AMQP_SESSION_UNATTACHED_HANDLE = "amqp:session:unattached-handle";
    public static final String AMQP_SESSION_WINDOW_VIOLATION = "amqp:session:window-violation";
    public static final String AMQP_SOURCE = "amqp:source:list";
    public static final String AMQP_TARGET = "amqp:target:list";
    public static final String AMQP_TRANSFER = "amqp:transfer:list";
    public static final String AMQP_TXN_COORDINATOR = "amqp:coordinator:list";
    public static final String AMQP_TXN_DECLARE = "amqp:declare:list";
    public static final String AMQP_TXN_DECLARED = "amqp:declared:list";
    public static final String AMQP_TXN_DISCHARGE = "amqp:discharge:list";
    public static final String AMQP_TXN_ROLLBACK = "amqp:transaction:rollback";
    public static final String AMQP_TXN_STATE = "amqp:transactional-state:list";
    public static final String AMQP_TXN_TIMEOUT = "amqp:transaction:timeout";
    public static final String AMQP_TXN_UNKNOWN_ID = "amqp:transaction:unknown-id";
    public static final String AMQP_VALUE = "amqp:amqp-value:*";
    public static final String ANONYMOUS_RELAY = "ANONYMOUS-RELAY";
    public static final String ANNOTATION_KEY = "x-opt-jms-msg-type";
    public static final String APACHE_LEGACY_DIRECT_BINDING = "apache.org:legacy-amqp-direct-binding:string";
    public static final String APACHE_LEGACY_NO_LOCAL_FILTER = "apache.org:jms-no-local-filter:list";
    public static final String APACHE_LEGACY_SELECTOR_FILTER = "apache.org:jms-selector-filter:string";
    public static final String APACHE_LEGACY_TOPIC_BINDING = "apache.org:legacy-amqp-topic-binding:string";
    public static final String APACHE_NO_LOCAL_FILTER = "apache.org:no-local-filter:list";
    public static final String APACHE_SELECTOR_FILTER = "apache.org:selector-filter:string";
    public static final String APP_OCTET_STREAM = "application/octet-stream";
    public static final String APP_X_JAVA_SERIALIZED_OBJ = "application/x-java-serialized-object";
    public static final String CONNECTION_CLOSE = "connection-close";
    public static final String CONTAINER_ID = "container-id";
    public static final String COPY = "copy";
    public static final String DELAYED_DELIVERY = "DELAYED_DELIVERY";
    public static final String DELIVERY_TAG = "delivery-tag";
    public static final String DELIVERY_TIME = "x-opt-delivery-time";
    public static final String DISCARD_UNROUTABLE = "DISCARD_UNROUTABLE";
    public static final String FIELD = "field";
    public static final String FILTER = "filter";
    public static final String GLOBAL_CAPABILITY = "global";
    public static final String INVALID_FIELD = "invalid-field";
    public static final String LIFETIME_POLICY = "lifetime-policy";
    public static final String LINK_DETACH = "link-detach";
    public static final String MOVE = "move";
    public static final String NETWORK_HOST = "network-host";
    public static final String NEVER = "never";
    public static final String NOT_VALID_BEFORE = "x-qpid-not-valid-before";
    public static final String PRIORITY = "priority";
    public static final String PORT = "port";
    public static final String PRODUCT = "product";
    public static final String REJECT_UNROUTABLE = "REJECT_UNROUTABLE";
    public static final String SHARED_CAPABILITY = "shared";
    public static final String SESSION_END = "session-end";
    public static final String SHARED_SUBSCRIPTIONS = "SHARED-SUBS";
    public static final String SOLE_CONNECTION_ENFORCEMENT = "sole-connection-enforcement";
    public static final String SOLE_CONNECTION_ENFORCEMENT_POLICY = "sole-connection-enforcement-policy";
    public static final String SOLE_CONNECTION_DETECTION_POLICY = "sole-connection-detection-policy";
    public static final String SOLE_CONNECTION_FOR_CONTAINER = "sole-connection-for-container";
    public static final String SUPPORTED_DIST_MODES = "supported-dist-modes";
    public static final String TEMPORARY_QUEUE = "temporary-queue";
    public static final String TEMPORARY_TOPIC = "temporary-topic";
    public static final String TOPIC = "topic";
    public static final String TXN_ID = "txn-id";
    public static final String VERSION = "version";
}
