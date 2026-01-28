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

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public interface QpidSymbols
{
    Symbol APACHE_LEGACY_DIRECT_BINDING = Symbol.valueOf(QpidSymbolTexts.APACHE_LEGACY_DIRECT_BINDING);
    Symbol APACHE_LEGACY_NO_LOCAL_FILTER = Symbol.valueOf(QpidSymbolTexts.APACHE_LEGACY_NO_LOCAL_FILTER);
    Symbol APACHE_LEGACY_SELECTOR_FILTER = Symbol.valueOf(QpidSymbolTexts.APACHE_LEGACY_SELECTOR_FILTER);
    Symbol APACHE_LEGACY_TOPIC_BINDING = Symbol.valueOf(QpidSymbolTexts.APACHE_LEGACY_TOPIC_BINDING);
    Symbol APACHE_NO_LOCAL_FILTER = Symbol.valueOf(QpidSymbolTexts.APACHE_NO_LOCAL_FILTER);
    Symbol APACHE_SELECTOR_FILTER = Symbol.valueOf(QpidSymbolTexts.APACHE_SELECTOR_FILTER);
    Symbol APP_OCTET_STREAM = Symbol.valueOf(QpidSymbolTexts.APP_OCTET_STREAM);
    Symbol APP_X_JAVA_SERIALIZED_OBJ = Symbol.valueOf(QpidSymbolTexts.APP_X_JAVA_SERIALIZED_OBJ);
    Symbol DELAYED_DELIVERY = Symbol.valueOf(QpidSymbolTexts.DELAYED_DELIVERY);
    Symbol DISCARD_UNROUTABLE = Symbol.valueOf(QpidSymbolTexts.DISCARD_UNROUTABLE);
    Symbol NOT_VALID_BEFORE = Symbol.valueOf(QpidSymbolTexts.NOT_VALID_BEFORE);
    Symbol REJECT_UNROUTABLE = Symbol.valueOf(QpidSymbolTexts.REJECT_UNROUTABLE);
}
