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

package org.apache.qpid.tests.protocol.v1_0.extensions.soleconn;

import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_DETECTION_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.CLOSE_EXISTING;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.REFUSE_CONNECTION;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assume.assumeThat;

import java.util.Collections;


import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionDetectionPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;

class SoleConnectionAsserts
{
    private static final Symbol CONNECTION_ESTABLISHMENT_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    private static final Symbol SOLE_CONNECTION_ENFORCEMENT = Symbol.valueOf("sole-connection-enforcement");
    private static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    private static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

    private SoleConnectionAsserts()
    {
    }

    static void assumeSoleConnectionCapability(Open open)
    {
        assumeThat(open.getOfferedCapabilities(), is(notNullValue()));
        assumeThat(open.getOfferedCapabilities(), hasItemInArray(SOLE_CONNECTION_FOR_CONTAINER));
    }

    static void assertSoleConnectionCapability(Open open)
    {
        assertThat(open.getOfferedCapabilities(), is(notNullValue()));
        assertThat(open.getOfferedCapabilities(), hasItemInArray(SOLE_CONNECTION_FOR_CONTAINER));
    }

    static void assumeEnforcementPolicyCloseExisting(Open open)
    {
        assumeThat(open.getProperties(), is(notNullValue()));
        assumeThat(open.getProperties(), hasEntry(SOLE_CONNECTION_ENFORCEMENT_POLICY, CLOSE_EXISTING.getValue()));
    }

    static void assertEnforcementPolicyCloseExisting(Open open)
    {
        assertThat(open.getProperties(), is(notNullValue()));
        assertThat(open.getProperties(), hasEntry(SOLE_CONNECTION_ENFORCEMENT_POLICY, CLOSE_EXISTING.getValue()));
    }

    static void assumeEnforcementPolicyRefuse(Open open)
    {
        assumeThat(open.getProperties(), is(notNullValue()));
        assumeThat(open.getProperties(),
                   anyOf(hasEntry(SOLE_CONNECTION_ENFORCEMENT_POLICY, REFUSE_CONNECTION.getValue()),
                         is(not(hasKey(SOLE_CONNECTION_ENFORCEMENT_POLICY)))));
    }

    static void assumeDetectionPolicyStrong(Open open)
    {
        assumeThat(open.getProperties(), is(notNullValue()));
        assumeThat(open.getProperties(),
                   anyOf(hasEntry(SOLE_CONNECTION_DETECTION_POLICY, SoleConnectionDetectionPolicy.STRONG.getValue()),
                         is(not(hasKey(SOLE_CONNECTION_DETECTION_POLICY)))));
    }

    static void assertConnectionEstablishmentFailed(final Open open)
    {
        assertThat(open.getProperties(), is(notNullValue()));
        assertThat(open.getProperties(), hasKey(CONNECTION_ESTABLISHMENT_FAILED));
        assertThat(open.getProperties(), hasEntry(CONNECTION_ESTABLISHMENT_FAILED, true));
    }

    static void assumeConnectionEstablishmentFailed(final Open open)
    {
        assumeThat(open.getProperties(), is(notNullValue()));
        assumeThat(open.getProperties(), hasKey(CONNECTION_ESTABLISHMENT_FAILED));
        assertThat(open.getProperties(), hasEntry(CONNECTION_ESTABLISHMENT_FAILED, true));
    }


    static void assertResourceLocked(final Close close)
    {
        assertThat(close.getError(), is(notNullValue()));
        assertThat(close.getError().getCondition(), is(equalTo(AmqpError.RESOURCE_LOCKED)));
        assertThat(close.getError().getInfo(), is(equalTo(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT, true))));
    }


    static void assertInvalidContainerId(final Close close)
    {
        assertThat(close.getError(), is(notNullValue()));
        assertThat(close.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
        assertThat(close.getError().getInfo(), is(equalTo(Collections.singletonMap(INVALID_FIELD, CONTAINER_ID))));
    }
}
