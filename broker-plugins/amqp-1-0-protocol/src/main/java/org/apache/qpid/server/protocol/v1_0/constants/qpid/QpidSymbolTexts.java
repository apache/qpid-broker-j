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

package org.apache.qpid.server.protocol.v1_0.constants.qpid;

public interface QpidSymbolTexts
{
    String APACHE_LEGACY_DIRECT_BINDING = "apache.org:legacy-amqp-direct-binding:string";
    String APACHE_LEGACY_NO_LOCAL_FILTER = "apache.org:jms-no-local-filter:list";
    String APACHE_LEGACY_SELECTOR_FILTER = "apache.org:jms-selector-filter:string";
    String APACHE_LEGACY_TOPIC_BINDING = "apache.org:legacy-amqp-topic-binding:string";
    String APACHE_NO_LOCAL_FILTER = "apache.org:no-local-filter:list";
    String APACHE_SELECTOR_FILTER = "apache.org:selector-filter:string";
    String APP_OCTET_STREAM = "application/octet-stream";
    String APP_X_JAVA_SERIALIZED_OBJ = "application/x-java-serialized-object";
    String DELAYED_DELIVERY = "DELAYED_DELIVERY";
    String DISCARD_UNROUTABLE = "DISCARD_UNROUTABLE";
    String NOT_VALID_BEFORE = "x-qpid-not-valid-before";
    String REJECT_UNROUTABLE = "REJECT_UNROUTABLE";
}
