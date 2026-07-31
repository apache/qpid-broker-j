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

package org.apache.qpid.server.protocol.v1_0.constants;

/** Protocol-wide constants defined by the AMQP 1.0 specification. */
public final class Constants
{
    private Constants()
    {
        // constructor is private for utility class
    }

    /**
     * The lower bound for the agreed maximum frame size, in bytes (AMQP 1.0 section 2.7.1,
     * MIN-MAX-FRAME-SIZE). A peer advertising a max-frame-size below this is in violation.
     */
    public static final int MIN_MAX_FRAME_SIZE = 512;
}
