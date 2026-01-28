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

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public interface MessagingSymbols
{
    Symbol AMQP_ACCEPTED = Symbol.valueOf(MessagingSymbolTexts.AMQP_ACCEPTED);
    Symbol AMQP_APPLICATION_PROPERTIES = Symbol.valueOf(MessagingSymbolTexts.AMQP_APPLICATION_PROPERTIES);
    Symbol AMQP_DATA = Symbol.valueOf(MessagingSymbolTexts.AMQP_DATA);
    Symbol AMQP_DELETE_ON_CLOSE = Symbol.valueOf(MessagingSymbolTexts.AMQP_DELETE_ON_CLOSE);
    Symbol AMQP_DELETE_ON_NO_LINKS = Symbol.valueOf(MessagingSymbolTexts.AMQP_DELETE_ON_NO_LINKS);
    Symbol AMQP_DELETE_ON_NO_LINKS_OR_MSGS = Symbol.valueOf(MessagingSymbolTexts.AMQP_DELETE_ON_NO_LINKS_OR_MSGS);
    Symbol AMQP_DELETE_ON_NO_MSGS = Symbol.valueOf(MessagingSymbolTexts.AMQP_DELETE_ON_NO_MSGS);
    Symbol AMQP_DELIVERY_ANNOTATIONS = Symbol.valueOf(MessagingSymbolTexts.AMQP_DELIVERY_ANNOTATIONS);
    Symbol AMQP_FOOTER = Symbol.valueOf(MessagingSymbolTexts.AMQP_FOOTER);
    Symbol AMQP_HEADER = Symbol.valueOf(MessagingSymbolTexts.AMQP_HEADER);
    Symbol AMQP_MESSAGE_ANNOTATIONS = Symbol.valueOf(MessagingSymbolTexts.AMQP_MESSAGE_ANNOTATIONS);
    Symbol AMQP_MODIFIED = Symbol.valueOf(MessagingSymbolTexts.AMQP_MODIFIED);
    Symbol AMQP_PROPERTIES = Symbol.valueOf(MessagingSymbolTexts.AMQP_PROPERTIES);
    Symbol AMQP_RECEIVED = Symbol.valueOf(MessagingSymbolTexts.AMQP_RECEIVED);
    Symbol AMQP_REJECTED = Symbol.valueOf(MessagingSymbolTexts.AMQP_REJECTED);
    Symbol AMQP_RELEASED = Symbol.valueOf(MessagingSymbolTexts.AMQP_RELEASED);
    Symbol AMQP_SEQUENCE = Symbol.valueOf(MessagingSymbolTexts.AMQP_SEQUENCE);
    Symbol AMQP_SOURCE = Symbol.valueOf(MessagingSymbolTexts.AMQP_SOURCE);
    Symbol AMQP_TARGET = Symbol.valueOf(MessagingSymbolTexts.AMQP_TARGET);
    Symbol AMQP_VALUE = Symbol.valueOf(MessagingSymbolTexts.AMQP_VALUE);
    Symbol ANNOTATION_KEY = Symbol.valueOf(MessagingSymbolTexts.ANNOTATION_KEY);
    Symbol CONNECTION_CLOSE = Symbol.valueOf(MessagingSymbolTexts.CONNECTION_CLOSE);
    Symbol COPY = Symbol.valueOf(MessagingSymbolTexts.COPY);
    Symbol DELIVERY_TAG = Symbol.valueOf(MessagingSymbolTexts.DELIVERY_TAG);
    Symbol DELIVERY_TIME = Symbol.valueOf(MessagingSymbolTexts.DELIVERY_TIME);
    Symbol LIFETIME_POLICY = Symbol.valueOf(MessagingSymbolTexts.LIFETIME_POLICY);
    Symbol LINK_DETACH = Symbol.valueOf(MessagingSymbolTexts.LINK_DETACH);
    Symbol MOVE = Symbol.valueOf(MessagingSymbolTexts.MOVE);
    Symbol NEVER = Symbol.valueOf(MessagingSymbolTexts.NEVER);
    Symbol PRIORITY = Symbol.valueOf(MessagingSymbolTexts.PRIORITY);
    Symbol SESSION_END = Symbol.valueOf(MessagingSymbolTexts.SESSION_END);
    Symbol SUPPORTED_DIST_MODES = Symbol.valueOf(MessagingSymbolTexts.SUPPORTED_DIST_MODES);
    Symbol TEMPORARY_QUEUE = Symbol.valueOf(MessagingSymbolTexts.TEMPORARY_QUEUE);
    Symbol TEMPORARY_TOPIC = Symbol.valueOf(MessagingSymbolTexts.TEMPORARY_TOPIC);
    Symbol TOPIC = Symbol.valueOf(MessagingSymbolTexts.TOPIC);
}
