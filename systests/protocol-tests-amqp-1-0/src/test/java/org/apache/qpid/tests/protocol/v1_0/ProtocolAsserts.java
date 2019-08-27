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

package org.apache.qpid.tests.protocol.v1_0;

import static org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError.NOT_IMPLEMENTED;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assume.assumeThat;

import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.tests.protocol.Response;

public class ProtocolAsserts
{
    private ProtocolAsserts()
    {
    }

    /**
     * When core spec is not vocal about how the error on attach can be reported,
     * there are potentially several ways of communicating error back to the client:
     * <pre>
     * Attach, Detach(with error)
     * Attach, Detach, End(with error)
     * Attach, Detach, End, Close(with error)
     * End(with error)
     * End, Close(with error)
     * </pre>
     * Thus, in order to assert the error possible codes, we need to get {@link ErrorCarryingFrameBody}
     * (implemented by {@link Detach}, {@link End}, {@link Close}) and examine error field there.
     * If error is set, than assert the error code, otherwise, receive subsequent {@link ErrorCarryingFrameBody}
     * and examine error field there.
     *
     * @param interaction interaction
     * @param expected possible errors
     */
    public static void assertAttachError(final Interaction interaction, final ErrorCondition... expected)
            throws Exception
    {
        Response<?> response = interaction.consumeResponse().getLatestResponse();
        assertThat(response, is(notNullValue()));
        Object responseBody = response.getBody();
        assertThat(responseBody, is(notNullValue()));

        if (response.getBody() instanceof Attach)
        {
            // expected either Detach or End or Close
            response = interaction.consumeResponse().getLatestResponse();
            assertThat(response, is(notNullValue()));
            responseBody = response.getBody();
            assertThat(responseBody, is(notNullValue()));
        }

        assertThat(responseBody, instanceOf(ErrorCarryingFrameBody.class));

        Error error = ((ErrorCarryingFrameBody) responseBody).getError();
        if (error != null)
        {
            assumeThat(error.getCondition(), is(not(NOT_IMPLEMENTED)));
            assertThat(error.getCondition(), oneOf(expected));
        }
        else
        {
            // expected either End or Close
            response = interaction.consumeResponse().getLatestResponse();
            assertThat(response, is(notNullValue()));
            Object nextBody = response.getBody();
            assertThat(nextBody, is(notNullValue()));
            assertThat(nextBody, instanceOf(ErrorCarryingFrameBody.class));
            error = ((ErrorCarryingFrameBody) nextBody).getError();
            if (error != null)
            {
                assumeThat(error.getCondition(), is(not(NOT_IMPLEMENTED)));
                assertThat(error.getCondition(), oneOf(expected));
            }
            else
            {
                // expected Close
                response = interaction.consumeResponse().getLatestResponse();
                assertThat(response, is(notNullValue()));
                Object body = response.getBody();
                assertThat(body, is(notNullValue()));
                assertThat(body, instanceOf(Close.class));
                error = ((ErrorCarryingFrameBody) body).getError();
                assertThat(error.getCondition(), is(notNullValue()));
                assumeThat(error.getCondition(), is(not(NOT_IMPLEMENTED)));
                assertThat(error.getCondition(), oneOf(expected));
            }
        }
    }

}
