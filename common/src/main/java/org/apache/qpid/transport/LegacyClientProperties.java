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
package org.apache.qpid.transport;

interface LegacyClientProperties
{
    String IDLE_TIMEOUT_PROP_NAME = "idle_timeout";
    String AMQJ_HEARTBEAT_TIMEOUT_FACTOR = "amqj.heartbeat.timeoutFactor";
    String AMQJ_DEFAULT_SYNCWRITE_TIMEOUT = "amqj.default_syncwrite_timeout";
    String AMQJ_TCP_NODELAY_PROP_NAME = "amqj.tcp_nodelay";
    String LEGACY_SEND_BUFFER_SIZE_PROP_NAME  = "amqj.sendBufferSize";
    String AMQJ_HEARTBEAT_DELAY = "amqj.heartbeat.delay";
    String QPID_SSL_KEY_STORE_CERT_TYPE_PROP_NAME = "qpid.ssl.keyStoreCertType";
    String QPID_SSL_TRUST_STORE_CERT_TYPE_PROP_NAME = "qpid.ssl.trustStoreCertType";
    String LEGACY_RECEIVE_BUFFER_SIZE_PROP_NAME  = "amqj.receiveBufferSize";
}
