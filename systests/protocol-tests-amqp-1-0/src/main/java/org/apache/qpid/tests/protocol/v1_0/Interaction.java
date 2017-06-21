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

import static com.google.common.util.concurrent.Futures.allAsList;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.SaslFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class Interaction
{
    private static final Set<String> CONTAINER_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Begin _begin;
    private final Open _open;
    private final Close _close;
    private final Attach _attach;
    private final Detach _detach;
    private final Flow _flow;
    private final Transfer _transfer;
    private final FrameTransport _transport;
    private final SaslInit _saslInit;
    private final SaslResponse _saslResponse;
    private byte[] _protocolHeader;
    private UnsignedShort _connectionChannel;
    private UnsignedShort _sessionChannel;
    private Response<?> _latestResponse;
    private ListenableFuture<?> _latestFuture;

    Interaction(final FrameTransport frameTransport)
    {
        final UnsignedInteger defaultLinkHandle = UnsignedInteger.ZERO;
        _transport = frameTransport;

        _protocolHeader = "AMQP\0\1\0\0".getBytes(UTF_8);

        _saslInit = new SaslInit();
        _saslResponse = new SaslResponse();

        _open = new Open();
        _open.setContainerId(getConnectionId());
        _close = new Close();
        _connectionChannel = UnsignedShort.valueOf(0);

        _begin = new Begin();
        _begin.setNextOutgoingId(UnsignedInteger.ZERO);
        _begin.setIncomingWindow(UnsignedInteger.ZERO);
        _begin.setOutgoingWindow(UnsignedInteger.ZERO);
        _sessionChannel = UnsignedShort.valueOf(1);

        _attach = new Attach();
        _attach.setName("testLink");
        _attach.setHandle(defaultLinkHandle);
        _attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        _attach.setSource(new Source());
        _attach.setTarget(new Target());

        _detach = new Detach();
        _detach.setHandle(_attach.getHandle());

        _flow = new Flow();
        _flow.setNextIncomingId(UnsignedInteger.ZERO);
        _flow.setIncomingWindow(UnsignedInteger.ZERO);
        _flow.setNextOutgoingId(UnsignedInteger.ZERO);
        _flow.setOutgoingWindow(UnsignedInteger.ZERO);

        _transfer = new Transfer();
        _transfer.setHandle(defaultLinkHandle);
        _transfer.setDeliveryTag(new Binary("testDeliveryTag".getBytes(StandardCharsets.UTF_8)));
        _transfer.setDeliveryId(UnsignedInteger.ZERO);
    }

    /////////////////////////
    // Protocol Negotiation //
    /////////////////////////

    public Interaction protocolHeader(byte[] header)
    {
        _protocolHeader = header;
        return this;
    }

    public Interaction negotiateProtocol() throws Exception
    {
        final ListenableFuture<Void> future = _transport.sendProtocolHeader(_protocolHeader);
        if (_latestFuture != null)
        {
            _latestFuture = allAsList(_latestFuture, future);
        }
        else
        {
            _latestFuture = future;
        }
        return this;
    }

    //////////
    // SASL //
    //////////

    public Interaction saslMechanism(final Symbol mechanism)
    {
        _saslInit.setMechanism(mechanism);
        return this;
    }

    public Interaction saslInitialResponse(final Binary initialResponse)
    {
        _saslInit.setInitialResponse(initialResponse);
        return this;
    }

    public Interaction saslInit() throws Exception
    {
        sendPerformativeAndChainFuture(_saslInit);
        return this;
    }

    public Interaction saslResponseResponse(Binary response)
    {
        _saslResponse.setResponse(response);
        return this;
    }

    public Interaction saslResponse() throws Exception
    {
        sendPerformativeAndChainFuture(_saslResponse);
        return this;
    }

    ////////////////
    // Connection //
    ////////////////

    public Interaction connectionChannel(UnsignedShort connectionChannel)
    {
        _connectionChannel = connectionChannel;
        return this;
    }

    public Interaction openContainerId(String containerId)
    {
        _open.setContainerId(containerId);
        return this;
    }

    public Interaction openHostname(String hostname)
    {
        _open.setHostname(hostname);
        return this;
    }

    public Interaction openChannelMax(UnsignedShort channelMax)
    {
        _open.setChannelMax(channelMax);
        return this;
    }

    public Interaction openDesiredCapabilities(final Symbol... desiredCapabilities)
    {
        _open.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public Interaction openProperties(final Map<Symbol, Object> properties)
    {
        _open.setProperties(properties);
        return this;
    }

    public Interaction open() throws Exception
    {
        sendPerformativeAndChainFuture(_open, _connectionChannel);
        return this;
    }

    public Interaction close() throws Exception
    {
        sendPerformativeAndChainFuture(_close, _connectionChannel);
        return this;
    }

    private String getConnectionId()
    {
        int index = 1;
        String containerId = String.format("testContainer-%d", index);
        while (CONTAINER_IDS.contains(containerId))
        {
            ++index;
            containerId = String.format("testContainer-%d", index);
        }
        CONTAINER_IDS.add(containerId);
        return containerId;
    }

    /////////////
    // Session //
    /////////////

    public Interaction sessionChannel(UnsignedShort sessionChannel)
    {
        _sessionChannel = sessionChannel;
        return this;
    }

    public Interaction beginNextOutgoingId(UnsignedInteger nextOutgoingId)
    {
        _begin.setNextOutgoingId(nextOutgoingId);
        return this;
    }

    public Interaction beginIncomingWindow(UnsignedInteger incomingWindow)
    {
        _begin.setIncomingWindow(incomingWindow);
        return this;
    }

    public Interaction beginOutgoingWindow(UnsignedInteger outgoingWindow)
    {
        _begin.setOutgoingWindow(outgoingWindow);
        return this;
    }

    public Interaction begin() throws Exception
    {
        sendPerformativeAndChainFuture(_begin, _sessionChannel);
        return this;
    }

    //////////
    // Link //
    //////////


    public Interaction attachName(String linkName)
    {
        _attach.setName(linkName);
        return this;
    }

    public Interaction attachRole(Role role)
    {
        _attach.setRole(role);
        return this;
    }

    public Interaction attachHandle(UnsignedInteger handle)
    {
        _attach.setHandle(handle);
        _detach.setHandle(handle);
        return this;
    }

    public Interaction attachInitialDeliveryCount(UnsignedInteger initialDeliveryCount)
    {
        _attach.setInitialDeliveryCount(initialDeliveryCount);
        return this;
    }

    public Interaction attachRcvSettleMode(final ReceiverSettleMode rcvSettleMode)
    {
        _attach.setRcvSettleMode(rcvSettleMode);
        return this;
    }

    public Interaction attachSource(Source source)
    {
        _attach.setSource(source);
        return this;
    }

    public Interaction attachTarget(BaseTarget target)
    {
        _attach.setTarget(target);
        return this;
    }

    public Interaction attachSourceAddress(String address)
    {
        Source source = (Source) _attach.getSource();
        source.setAddress(address);
        _attach.setSource(source);
        return this;
    }

    public Interaction attachSourceOutcomes(final Symbol... outcomes)
    {
        Source source = ((Source) _attach.getSource());
        source.setOutcomes(outcomes);
        _attach.setSource(source);
        return this;
    }

    public Interaction attachTargetAddress(final String address)
    {
        final Target target = ((Target) _attach.getTarget());
        target.setAddress(address);
        _attach.setTarget(target);
        return this;
    }

    public Interaction attach() throws Exception
    {
        sendPerformativeAndChainFuture(_attach, _sessionChannel);
        return this;
    }

    public Interaction detachClose(Boolean close)
    {
        _detach.setClosed(close);
        return this;
    }

    public Interaction detach() throws Exception
    {
        sendPerformativeAndChainFuture(_detach, _sessionChannel);
        return this;
    }

    //////////
    // FLow //
    //////////

    public Interaction flowIncomingWindow(final UnsignedInteger incomingWindow)
    {
        _flow.setIncomingWindow(incomingWindow);
        return this;
    }

    public Interaction flowNextIncomingId(final UnsignedInteger nextIncomingId)
    {
        _flow.setNextIncomingId(nextIncomingId);
        return this;
    }

    public Interaction flowOutgoingWindow(final UnsignedInteger outgoingWindow)
    {
        _flow.setOutgoingWindow(outgoingWindow);
        return this;
    }

    public Interaction flowNextOutgoingId(final UnsignedInteger nextNextOutgoingId)
    {
        _flow.setNextOutgoingId(nextNextOutgoingId);
        return this;
    }

    public Interaction flowEcho(final Boolean echo)
    {
        _flow.setEcho(echo);
        return this;
    }

    public Interaction flowHandle(final UnsignedInteger handle)
    {
        _flow.setHandle(handle);
        return this;
    }

    public Interaction flowAvailable(final UnsignedInteger available)
    {
        _flow.setAvailable(available);
        return this;
    }

    public Interaction flowDeliveryCount(final UnsignedInteger deliveryCount)
    {
        _flow.setDeliveryCount(deliveryCount);
        return this;
    }

    public Interaction flowLinkCredit(final UnsignedInteger linkCredit)
    {
        _flow.setLinkCredit(linkCredit);
        return this;
    }

    public Interaction flowDrain(final Boolean drain)
    {
        _flow.setDrain(drain);
        return this;
    }

    public Interaction flow() throws Exception
    {
        sendPerformativeAndChainFuture(_flow, _sessionChannel);
        return this;
    }

    //////////////
    // Transfer //
    //////////////

    public Interaction transferHandle(UnsignedInteger transferHandle)
    {
        _transfer.setHandle(transferHandle);
        return this;
    }

    public Interaction transferDeliveryTag(final Binary deliveryTag)
    {
        _transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public Interaction transferDeliveryId(final UnsignedInteger deliveryId)
    {
        _transfer.setDeliveryId(deliveryId);
        return this;
    }

    public Interaction transferRcvSettleMode(final ReceiverSettleMode receiverSettleMode)
    {
        _transfer.setRcvSettleMode(receiverSettleMode);
        return this;
    }

    public Interaction transferMore(final Boolean more)
    {
        _transfer.setMore(more);
        return this;
    }

    public Interaction transferMessageFormat(final UnsignedInteger messageFormat)
    {
        _transfer.setMessageFormat(messageFormat);
        return this;
    }

    public Interaction transferPayload(final List<QpidByteBuffer> payload)
    {
        _transfer.setPayload(payload);
        return this;
    }

    public Interaction transferPayloadData(final Object payload)
    {
        AmqpValue amqpValue = new AmqpValue(payload);
        final AmqpValueSection section = amqpValue.createEncodingRetainingSection();
        final List<QpidByteBuffer> encodedForm = section.getEncodedForm();
        _transfer.setPayload(encodedForm);

        section.dispose();
        for (QpidByteBuffer qbb : encodedForm)
        {
            qbb.dispose();
        }
        return this;
    }

    public Interaction transferSettled(final Boolean settled)
    {
        _transfer.setSettled(settled);
        return this;
    }

    public Interaction transfer() throws Exception
    {
        sendPerformativeAndChainFuture(_transfer, _sessionChannel);
        return this;
    }

    //////////
    // misc //
    //////////

    public Interaction sendPerformative(final FrameBody frameBody,
                                        final UnsignedShort channel) throws Exception
    {
        sendPerformativeAndChainFuture(frameBody, channel);
        return this;
    }

    public Interaction sendPerformative(final SaslFrameBody saslFrameBody) throws Exception
    {
        sendPerformativeAndChainFuture(saslFrameBody);
        return this;
    }

    private void sendPerformativeAndChainFuture(final SaslFrameBody frameBody) throws Exception
    {
        final ListenableFuture<Void> future = _transport.sendPerformative(frameBody);
        if (_latestFuture != null)
        {
            _latestFuture = allAsList(_latestFuture, future);
        }
        else
        {
            _latestFuture = future;
        }
    }

    private void sendPerformativeAndChainFuture(final FrameBody frameBody, final UnsignedShort channel) throws Exception
    {
        final ListenableFuture<Void> future = _transport.sendPerformative(frameBody, channel);
        if (_latestFuture != null)
        {
            _latestFuture = allAsList(_latestFuture, future);
        }
        else
        {
            _latestFuture = future;
        }
    }

    public Interaction consumeResponse(final Class<?>... responseTypes) throws Exception
    {
        sync();
        _latestResponse = _transport.getNextResponse();
        final Set<Class<?>> acceptableResponseClasses = new HashSet<>(Arrays.asList(responseTypes));
        if ((acceptableResponseClasses.isEmpty() && _latestResponse != null)
            || (acceptableResponseClasses.contains(null) && _latestResponse == null))
        {
            return this;
        }
        acceptableResponseClasses.remove(null);
        for (Class<?> acceptableResponseClass : acceptableResponseClasses)
        {
            if (acceptableResponseClass.isAssignableFrom(_latestResponse.getBody().getClass()))
            {
                return this;
            }
        }
        throw new IllegalStateException(String.format("Unexpected response. Expected one of '%s' got '%s'.",
                                                      acceptableResponseClasses,
                                                      _latestResponse == null ? null : _latestResponse.getBody()));
    }

    public Interaction sync() throws InterruptedException, ExecutionException, TimeoutException
    {
        if (_latestFuture != null)
        {
            _latestFuture.get(FrameTransport.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
            _latestFuture = null;
        }
        return this;
    }

    public Response<?> getLatestResponse() throws Exception
    {
        sync();
        return _latestResponse;
    }

    public <T> T getLatestResponse(Class<T> type) throws Exception
    {
        sync();
        if (!type.isAssignableFrom(_latestResponse.getBody().getClass()))
        {
            throw new IllegalStateException(String.format("Unexpected response. Expected '%s' got '%s'.",
                                                          type.getSimpleName(),
                                                          _latestResponse.getBody()));
        }

        return (T) _latestResponse.getBody();
    }

    public Interaction flowHandleFromLinkHandle()
    {
        _flow.setHandle(_attach.getHandle());
        return this;
    }
}
