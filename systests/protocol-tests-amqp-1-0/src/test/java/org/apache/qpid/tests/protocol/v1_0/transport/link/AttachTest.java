/*
 * Copyright 2017 by Lorenz Quack
 *
 * This file is part of qpid-java-build.
 *
 *     qpid-java-build is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 2 of the License, or
 *     (at your option) any later version.
 *
 *     qpid-java-build is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with qpid-java-build.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.apache.qpid.tests.protocol.v1_0.transport.link;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class AttachTest extends ProtocolTestBase
{
    @Test
    @SpecificationTest(section = "1.3.4",
            description = "Attach without mandatory fields should result in a decoding error.")
    public void emptyAttach() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr))
        {
            transport.doBeginSession();
            Attach attach = new Attach();

            transport.sendPerformative(attach);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Close.class)));
            Close responseClose = (Close) response.getFrameBody();
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(AmqpError.DECODE_ERROR));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Links are established and/or resumed by creating a link endpoint associated with a local terminus, "
                          + "assigning it to an unused handle, and sending an attach frame.")
    public void successfulAttachAsSender() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr))
        {
            Role localRole = Role.SENDER;
            transport.doBeginSession();
            Attach attach = new Attach();
            attach.setName("testLink");
            attach.setHandle(new UnsignedInteger(0));
            attach.setRole(localRole);
            attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
            Source source = new Source();
            attach.setSource(source);
            Target target = new Target();
            attach.setTarget(target);

            transport.sendPerformative(attach);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Attach.class)));
            Attach responseAttach = (Attach) response.getFrameBody();
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));
            assertThat(responseAttach.getTarget(), is(notNullValue()));
            assertThat(responseAttach.getSource(), is(notNullValue()));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Links are established and/or resumed by creating a link endpoint associated with a local terminus, "
                          + "assigning it to an unused handle, and sending an attach frame.")
    public void successfulAttachAsReceiver() throws Exception
    {
        String queueName = "testQueue";
        getBrokerAdmin().createQueue(queueName);
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr))
        {
            Role localRole = Role.RECEIVER;
            transport.doBeginSession();
            Attach attach = new Attach();
            attach.setName("testLink");
            attach.setHandle(new UnsignedInteger(0));
            attach.setRole(localRole);
            Source source = new Source();
            source.setAddress(queueName);
            attach.setSource(source);
            Target target = new Target();
            attach.setTarget(target);

            transport.sendPerformative(attach);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Attach.class)));
            Attach responseAttach = (Attach) response.getFrameBody();
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));
            assertThat(responseAttach.getSource(), is(notNullValue()));
        }
    }
}
