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

package org.apache.qpid.server.protocol.v1_0.constants.amqp.messaging;

public interface MessagingSymbolTexts
{
    String AMQP_ACCEPTED = "amqp:accepted:list";
    String AMQP_APPLICATION_PROPERTIES = "amqp:application-properties:map";
    String AMQP_DATA = "amqp:data:binary";
    String AMQP_DELETE_ON_CLOSE = "amqp:delete-on-close:list";
    String AMQP_DELETE_ON_NO_LINKS = "amqp:delete-on-no-links:list";
    String AMQP_DELETE_ON_NO_LINKS_OR_MSGS = "amqp:delete-on-no-links-or-messages:list";
    String AMQP_DELETE_ON_NO_MSGS = "amqp:delete-on-no-messages:list";
    String AMQP_DELIVERY_ANNOTATIONS = "amqp:delivery-annotations:map";
    String AMQP_FOOTER = "amqp:footer:map";
    String AMQP_HEADER = "amqp:header:list";
    String AMQP_MESSAGE_ANNOTATIONS = "amqp:message-annotations:map";
    String AMQP_MODIFIED = "amqp:modified:list";
    String AMQP_PROPERTIES = "amqp:properties:list";
    String AMQP_RECEIVED = "amqp:received:list";
    String AMQP_REJECTED = "amqp:rejected:list";
    String AMQP_RELEASED = "amqp:released:list";
    String AMQP_SEQUENCE = "amqp:amqp-sequence:list";
    String AMQP_SOURCE = "amqp:source:list";
    String AMQP_TARGET = "amqp:target:list";
    String AMQP_VALUE = "amqp:amqp-value:*";
    String CONNECTION_CLOSE = "connection-close";
    String ANNOTATION_KEY = "x-opt-jms-msg-type";
    String COPY = "copy";
    String DELIVERY_TAG = "delivery-tag";
    String DELIVERY_TIME = "x-opt-delivery-time";
    String LIFETIME_POLICY = "lifetime-policy";
    String LINK_DETACH = "link-detach";
    String MOVE = "move";
    String NEVER = "never";
    String PRIORITY = "priority";
    String SESSION_END = "session-end";
    String SUPPORTED_DIST_MODES = "supported-dist-modes";
    String TEMPORARY_QUEUE = "temporary-queue";
    String TEMPORARY_TOPIC = "temporary-topic";
    String TOPIC = "topic";
}
