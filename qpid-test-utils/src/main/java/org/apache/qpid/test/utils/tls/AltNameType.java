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

package org.apache.qpid.test.utils.tls;

public enum AltNameType
{
    // GeneralName tag values from X.509 SubjectAlternativeName.
    OTHER_NAME(0),
    RFC822_NAME(1),
    DNS_NAME(2),
    X400_ADDRESS(3),
    DIRECTORY_NAME(4),
    EDI_PARTY_NAME(5),
    UNIFORM_RESOURCE_IDENTIFIER(6),
    IP_ADDRESS(7),
    REGISTERED_ID(8);

    private final int _generalNameTag;

    AltNameType(final int generalNameTag)
    {
        _generalNameTag = generalNameTag;
    }

    public int generalNameTag()
    {
        return _generalNameTag;
    }
}
