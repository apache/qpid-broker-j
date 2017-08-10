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

package org.apache.qpid.server.message.mimecontentconverter;

import java.util.regex.Pattern;

public class ConversionUtils
{
    public static final Pattern
            TEXT_CONTENT_TYPES = Pattern.compile("^(text/.*)|(application/(xml|xml-dtd|.*\\+xml|json|.*\\+json|javascript|ecmascript))$");
    public static final Pattern MAP_MESSAGE_CONTENT_TYPES = Pattern.compile("^amqp/map|jms/map-message$");
    public static final Pattern LIST_MESSAGE_CONTENT_TYPES = Pattern.compile("^amqp/list|jms/stream-message$");
    public static final Pattern
            OBJECT_MESSAGE_CONTENT_TYPES = Pattern.compile("^application/x-java-serialized-object|application/java-object-stream$");
    public static final Pattern BYTES_MESSAGE_CONTENT_TYPES = Pattern.compile("^application/octet-stream$");
}
