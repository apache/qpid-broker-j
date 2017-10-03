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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.SaslFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class Interaction
{
    private static final Set<String> CONTAINER_IDS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Begin _begin;
    private final End _end;
    private final Open _open;
    private final Close _close;
    private final Attach _attach;
    private final Detach _detach;
    private final Flow _flow;
    private final Transfer _transfer;
    private final Disposition _disposition;
    private final FrameTransport _transport;
    private final SaslInit _saslInit;
    private final SaslResponse _saslResponse;
    private byte[] _protocolHeader;
    private UnsignedShort _connectionChannel;
    private UnsignedShort _sessionChannel;
    private Response<?> _latestResponse;
    private ListenableFuture<?> _latestFuture;
    private int _deliveryIdCounter;
    private List<Transfer> _latestDelivery;
    private Object _decodedLatestDelivery;

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
        _end = new End();
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
        _transfer.setDeliveryId(getNextDeliveryId());

        _disposition = new Disposition();
        _disposition.setFirst(UnsignedInteger.ZERO);
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
        sendPerformativeAndChainFuture(copySaslInit(_saslInit));
        return this;
    }

    private SaslInit copySaslInit(final SaslInit saslInit)
    {
        final SaslInit saslInitCopy = new SaslInit();
        saslInitCopy.setMechanism(saslInit.getMechanism());
        saslInitCopy.setInitialResponse(saslInit.getInitialResponse());
        saslInitCopy.setHostname(saslInit.getHostname());
        return saslInitCopy;
    }

    public Interaction saslResponseResponse(Binary response)
    {
        _saslResponse.setResponse(response);
        return this;
    }

    public Interaction saslResponse() throws Exception
    {
        sendPerformativeAndChainFuture(copySaslResponse(_saslResponse));
        return this;
    }

    private SaslResponse copySaslResponse(final SaslResponse saslResponse)
    {
        final SaslResponse saslResponseCopy = new SaslResponse();
        saslResponseCopy.setResponse(saslResponse.getResponse());
        return saslResponseCopy;
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

    public Interaction openMaxFrameSize(final UnsignedInteger maxFrameSize)
    {
        _open.setMaxFrameSize(maxFrameSize);
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
        sendPerformativeAndChainFuture(copyOpen(_open), _connectionChannel);
        return this;
    }

    private Open copyOpen(final Open open)
    {
        final Open openCopy = new Open();
        openCopy.setContainerId(open.getContainerId());
        openCopy.setHostname(open.getHostname());
        openCopy.setMaxFrameSize(open.getMaxFrameSize());
        openCopy.setChannelMax(open.getChannelMax());
        openCopy.setIdleTimeOut(open.getIdleTimeOut());
        openCopy.setOutgoingLocales(open.getOutgoingLocales());
        openCopy.setIncomingLocales(open.getIncomingLocales());
        openCopy.setOfferedCapabilities(open.getOfferedCapabilities());
        openCopy.setDesiredCapabilities(open.getDesiredCapabilities());
        if (open.getProperties() != null)
        {
            openCopy.setProperties(new LinkedHashMap<>(open.getProperties()));
        }
        return openCopy;
    }

    public Interaction close() throws Exception
    {
        sendPerformativeAndChainFuture(copyClose(_close), _connectionChannel);
        return this;
    }

    private Close copyClose(final Close close)
    {
        final Close closeCopy = new Close();
        closeCopy.setError(close.getError());
        return closeCopy;
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
        sendPerformativeAndChainFuture(copyBegin(_begin), _sessionChannel);
        return this;
    }

    private Begin copyBegin(final Begin begin)
    {
        final Begin beginCopy = new Begin();
        beginCopy.setRemoteChannel(begin.getRemoteChannel());
        beginCopy.setNextOutgoingId(begin.getNextOutgoingId());
        beginCopy.setIncomingWindow(begin.getIncomingWindow());
        beginCopy.setOutgoingWindow(begin.getOutgoingWindow());
        beginCopy.setHandleMax(begin.getHandleMax());
        beginCopy.setOfferedCapabilities(begin.getOfferedCapabilities());
        beginCopy.setDesiredCapabilities(begin.getDesiredCapabilities());
        if (begin.getProperties() != null)
        {
            beginCopy.setProperties(new LinkedHashMap<>(begin.getProperties()));
        }
        return beginCopy;
    }

    public Interaction end() throws Exception
    {
        sendPerformativeAndChainFuture(copyEnd(_end), _sessionChannel);
        return this;
    }

    private End copyEnd(final End end)
    {
        final End endCopy = new End();
        endCopy.setError(end.getError());
        return endCopy;
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

    public Interaction attachSndSettleMode(final SenderSettleMode senderSettleMode)
    {
        _attach.setSndSettleMode(senderSettleMode);
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

    public Interaction attachSourceDefaultOutcome(final Outcome defaultOutcome)
    {
        Source source = ((Source) _attach.getSource());
        source.setDefaultOutcome(defaultOutcome);
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

    public Interaction attachUnsettled(final Map<Binary, DeliveryState> unsettled)
    {
        _attach.setUnsettled(unsettled);
        return this;
    }

    public Interaction attachIncompleteUnsettled(final Boolean incompleteUnsettled)
    {
        _attach.setIncompleteUnsettled(incompleteUnsettled);
        return this;
    }

    public Interaction attach() throws Exception
    {
        sendPerformativeAndChainFuture(copyAttach(_attach), _sessionChannel);
        return this;
    }

    private Attach copyAttach(final Attach attach)
    {
        final Attach attachCopy = new Attach();
        attachCopy.setName(attach.getName());
        attachCopy.setHandle(attach.getHandle());
        attachCopy.setRole(attach.getRole());
        attachCopy.setSndSettleMode(attach.getSndSettleMode());
        attachCopy.setRcvSettleMode(attach.getRcvSettleMode());
        final BaseSource baseSource = attach.getSource();
        if (baseSource != null && baseSource instanceof Source)
        {
            final Source source = (Source) baseSource;
            final Source sourceCopy = new Source();
            sourceCopy.setAddress(source.getAddress());
            sourceCopy.setDurable(source.getDurable());
            sourceCopy.setExpiryPolicy(source.getExpiryPolicy());
            sourceCopy.setTimeout(source.getTimeout());
            sourceCopy.setDynamic(source.getDynamic());
            if (source.getDynamicNodeProperties() != null)
            {
                sourceCopy.setDynamicNodeProperties(new LinkedHashMap<>(source.getDynamicNodeProperties()));
            }
            sourceCopy.setFilter(source.getFilter());
            sourceCopy.setDefaultOutcome(source.getDefaultOutcome());
            sourceCopy.setOutcomes(source.getOutcomes());
            sourceCopy.setCapabilities(source.getCapabilities());
            attachCopy.setSource(sourceCopy);
        }
        else
        {
            attachCopy.setSource(baseSource);
        }
        final BaseTarget baseTarget = attach.getTarget();
        if (baseTarget != null && baseTarget instanceof Target)
        {
            final Target target = (Target) baseTarget;
            final Target targetCopy = new Target();
            targetCopy.setAddress(target.getAddress());
            targetCopy.setDurable(target.getDurable());
            targetCopy.setExpiryPolicy(target.getExpiryPolicy());
            targetCopy.setTimeout(target.getTimeout());
            targetCopy.setDynamic(target.getDynamic());
            if (target.getDynamicNodeProperties() != null)
            {
                targetCopy.setDynamicNodeProperties(new LinkedHashMap<>(target.getDynamicNodeProperties()));
            }
            targetCopy.setCapabilities(target.getCapabilities());
            attachCopy.setTarget(targetCopy);
        }
        else
        {
            attachCopy.setTarget(baseTarget);
        }
        if (attach.getUnsettled() != null)
        {
            attachCopy.setUnsettled(new LinkedHashMap<>(attach.getUnsettled()));
        }
        attachCopy.setIncompleteUnsettled(attach.getIncompleteUnsettled());
        attachCopy.setInitialDeliveryCount(attach.getInitialDeliveryCount());
        attachCopy.setMaxMessageSize(attach.getMaxMessageSize());
        attachCopy.setOfferedCapabilities(attach.getOfferedCapabilities());
        attachCopy.setDesiredCapabilities(attach.getDesiredCapabilities());
        if (attach.getProperties() != null)
        {
            attachCopy.setProperties(new LinkedHashMap<>(attach.getProperties()));
        }
        return attachCopy;
    }

    public Interaction detachClose(Boolean close)
    {
        _detach.setClosed(close);
        return this;
    }

    public Interaction detach() throws Exception
    {
        sendPerformativeAndChainFuture(copyDetach(_detach), _sessionChannel);
        return this;
    }

    private Detach copyDetach(final Detach detach)
    {
        final Detach detachCopy = new Detach();
        detachCopy.setHandle(detach.getHandle());
        detachCopy.setClosed(detach.getClosed());
        detachCopy.setError(detach.getError());
        return detachCopy;
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

    public Interaction flowProperties(Map<Symbol, Object> properties)
    {
        _flow.setProperties(properties);
        return this;
    }

    public Interaction flow() throws Exception
    {
        sendPerformativeAndChainFuture(copyFlow(_flow), _sessionChannel);
        return this;
    }

    private Flow copyFlow(final Flow flow)
    {
        final Flow flowCopy = new Flow();
        flowCopy.setNextIncomingId(flow.getNextIncomingId());
        flowCopy.setIncomingWindow(flow.getIncomingWindow());
        flowCopy.setNextOutgoingId(flow.getNextOutgoingId());
        flowCopy.setOutgoingWindow(flow.getOutgoingWindow());
        flowCopy.setHandle(flow.getHandle());
        flowCopy.setDeliveryCount(flow.getDeliveryCount());
        flowCopy.setLinkCredit(flow.getLinkCredit());
        flowCopy.setAvailable(flow.getAvailable());
        flowCopy.setDrain(flow.getDrain());
        flowCopy.setEcho(flow.getEcho());
        if (flow.getProperties() != null)
        {
            flowCopy.setProperties(new LinkedHashMap<>(flow.getProperties()));
        }
        return flowCopy;
    }

    //////////////
    // Transfer //
    //////////////

    public Interaction transferHandle(UnsignedInteger transferHandle)
    {
        _transfer.setHandle(transferHandle);
        return this;
    }

    public Interaction transferDeliveryId(final UnsignedInteger deliveryId)
    {
        _transfer.setDeliveryId(deliveryId);
        return this;
    }

    public Interaction transferDeliveryTag(final Binary deliveryTag)
    {
        _transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public Interaction transferMessageFormat(final UnsignedInteger messageFormat)
    {
        _transfer.setMessageFormat(messageFormat);
        return this;
    }

    public Interaction transferSettled(final Boolean settled)
    {
        _transfer.setSettled(settled);
        return this;
    }

    public Interaction transferMore(final Boolean more)
    {
        _transfer.setMore(more);
        return this;
    }

    public Interaction transferRcvSettleMode(final ReceiverSettleMode receiverSettleMode)
    {
        _transfer.setRcvSettleMode(receiverSettleMode);
        return this;
    }

    public Interaction transferState(final DeliveryState state)
    {
        _transfer.setState(state);
        return this;
    }

    public Interaction transferTransactionalState(final Binary transactionalId)
    {
        TransactionalState transactionalState = new TransactionalState();
        transactionalState.setTxnId(transactionalId);
        return transferState(transactionalState);
    }

    public Interaction transferResume(final Boolean resume)
    {
        _transfer.setResume(resume);
        return this;
    }

    public Interaction transferAborted(final Boolean aborted)
    {
        _transfer.setAborted(aborted);
        return this;
    }

    public Interaction transferPayload(final List<QpidByteBuffer> payload)
    {
        _transfer.setPayload(payload);
        return this;
    }

    public Interaction transferPayloadData(final Object payload)
    {
        transferPayload(_transfer, payload);
        return this;
    }

    private void transferPayload(final Transfer transfer, final Object payload)
    {
        AmqpValue amqpValue = new AmqpValue(payload);
        final AmqpValueSection section = amqpValue.createEncodingRetainingSection();
        final List<QpidByteBuffer> encodedForm = section.getEncodedForm();
        transfer.setPayload(encodedForm);
        section.dispose();
        for (QpidByteBuffer qbb : encodedForm)
        {
            qbb.dispose();
        }
    }

    public Interaction transfer() throws Exception
    {
        sendPerformativeAndChainFuture(copyTransfer(_transfer), _sessionChannel);
        return this;
    }

    private Transfer copyTransfer(final Transfer transfer)
    {
        final Transfer transferCopy = new Transfer();
        transferCopy.setHandle(transfer.getHandle());
        transferCopy.setDeliveryId(transfer.getDeliveryId());
        transferCopy.setDeliveryTag(transfer.getDeliveryTag());
        transferCopy.setMessageFormat(transfer.getMessageFormat());
        transferCopy.setSettled(transfer.getSettled());
        transferCopy.setMore(transfer.getMore());
        transferCopy.setRcvSettleMode(transfer.getRcvSettleMode());
        transferCopy.setState(transfer.getState());
        transferCopy.setResume(transfer.getResume());
        transferCopy.setAborted(transfer.getAborted());
        transferCopy.setBatchable(transfer.getBatchable());
        final List<QpidByteBuffer> payload = transfer.getPayload();
        if (payload != null)
        {
            final List<QpidByteBuffer> payloadCopy = new ArrayList<>(payload.size());
            for (QpidByteBuffer qpidByteBuffer : payload)
            {
                payloadCopy.add(qpidByteBuffer.duplicate());
            }
            transferCopy.setPayload(payloadCopy);
        }
        return transferCopy;
    }

    /////////////////
    // disposition //
    /////////////////

    public Interaction dispositionSettled(final boolean settled)
    {
        _disposition.setSettled(settled);
        return this;
    }

    public Interaction dispositionState(final DeliveryState state)
    {
        _disposition.setState(state);
        return this;
    }


    public Interaction dispositionTransactionalState(final Binary transactionId, final Outcome outcome)
    {
        TransactionalState state = new TransactionalState();
        state.setTxnId(transactionId);
        state.setOutcome(outcome);
        return dispositionState(state);
    }

    public Interaction dispositionRole(final Role role)
    {
        _disposition.setRole(role);
        return this;
    }

    public Interaction dispositionFirst(final UnsignedInteger deliveryId)
    {
        _disposition.setFirst(deliveryId);
        return this;
    }

    public Interaction disposition() throws Exception
    {
        sendPerformativeAndChainFuture(copyDisposition(_disposition), _sessionChannel);
        return this;
    }

    private Disposition copyDisposition(final Disposition disposition)
    {
        final Disposition dispositionCopy = new Disposition();
        dispositionCopy.setRole(disposition.getRole());
        dispositionCopy.setFirst(disposition.getFirst());
        dispositionCopy.setLast(disposition.getLast());
        dispositionCopy.setSettled(disposition.getSettled());
        dispositionCopy.setState(disposition.getState());
        dispositionCopy.setBatchable(disposition.getBatchable());
        return dispositionCopy;
    }

    /////////////////
    // transaction //
    ////////////////

    public Interaction txnAttachCoordinatorLink(InteractionTransactionalState transactionalState) throws Exception
    {
        Attach attach = new Attach();
        attach.setName("testTransactionCoordinator-" + transactionalState.getHandle());
        attach.setHandle(transactionalState.getHandle());
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        attach.setTarget(new Coordinator());
        attach.setRole(Role.SENDER);
        Source source = new Source();
        attach.setSource(source);
        source.setOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL);
        sendPerformativeAndChainFuture(attach, _sessionChannel);
        consumeResponse(Attach.class);
        consumeResponse(Flow.class);
        return this;
    }

    public Interaction txnDeclare(final InteractionTransactionalState txnState) throws Exception
    {
        Transfer transfer = createTransactionTransfer(txnState.getHandle());
        transferPayload(transfer, new Declare());
        sendPerformativeAndChainFuture(transfer, _sessionChannel);
        consumeResponse(Disposition.class);
        Disposition declareTransactionDisposition = getLatestResponse(Disposition.class);
        assertThat(declareTransactionDisposition.getSettled(), is(equalTo(true)));
        assertThat(declareTransactionDisposition.getState(), is(instanceOf(Declared.class)));
        Binary transactionId = ((Declared) declareTransactionDisposition.getState()).getTxnId();
        assertThat(transactionId, is(notNullValue()));
        consumeResponse(Flow.class);
        txnState.setLastTransactionId(transactionId);
        return this;
    }

    public Interaction txnDischarge(final InteractionTransactionalState txnState, boolean failed) throws Exception
    {
        final Discharge discharge = new Discharge();
        discharge.setTxnId(txnState.getCurrentTransactionId());
        discharge.setFail(failed);

        Transfer transfer = createTransactionTransfer(txnState.getHandle());
        transferPayload(transfer, discharge);
        sendPerformativeAndChainFuture(transfer, _sessionChannel);

        Disposition declareTransactionDisposition = null;
        Flow coordinatorFlow = null;
        do
        {
            consumeResponse(Disposition.class, Flow.class);
            Response<?> response = getLatestResponse();
            if (response.getBody() instanceof Disposition)
            {
                declareTransactionDisposition = (Disposition) response.getBody();
            }
            if (response.getBody() instanceof Flow)
            {
                final Flow flowResponse = (Flow) response.getBody();
                if (flowResponse.getHandle().equals(txnState.getHandle()))
                {
                    coordinatorFlow = flowResponse;
                }
            }
        } while(declareTransactionDisposition == null || coordinatorFlow == null);

        assertThat(declareTransactionDisposition.getSettled(), is(equalTo(true)));
        assertThat(declareTransactionDisposition.getState(), is(instanceOf(Accepted.class)));

        txnState.setLastTransactionId(null);
        return this;
    }

    private Transfer createTransactionTransfer(final UnsignedInteger handle)
    {
        Transfer transfer = new Transfer();
        transfer.setHandle(handle);
        transfer.setDeliveryId(getNextDeliveryId());
        transfer.setDeliveryTag(new Binary(("transaction-" + transfer.getDeliveryId()).getBytes(StandardCharsets.UTF_8)));
        return transfer;
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
        if (_latestResponse != null)
        {
            for (Class<?> acceptableResponseClass : acceptableResponseClasses)
            {
                if (acceptableResponseClass.isAssignableFrom(_latestResponse.getBody().getClass()))
                {
                    return this;
                }
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

    private UnsignedInteger getNextDeliveryId()
    {
        return UnsignedInteger.valueOf(_deliveryIdCounter++);
    }

    public Interaction receiveDelivery() throws Exception
    {
        sync();
        _latestDelivery = receiveAllTransfers();
        return this;
    }

    public Interaction decodeLatestDelivery() throws AmqpErrorException
    {
        MessageDecoder messageDecoder = new MessageDecoder();
        _latestDelivery.forEach(transfer ->
                                {
                                    messageDecoder.addTransfer(transfer);
                                    transfer.dispose();
                                });
        _decodedLatestDelivery = messageDecoder.getData();
        _latestDelivery = null;
        return this;
    }

    public List<Transfer> getLatestDelivery()
    {
        return _latestDelivery;
    }

    public Object getDecodedLatestDelivery()
    {
        return _decodedLatestDelivery;
    }

    private List<Transfer> receiveAllTransfers() throws Exception
    {
        List<Transfer> transfers = new ArrayList<>();
        boolean hasMore;
        do
        {
            Transfer responseTransfer = consumeResponse().getLatestResponse(Transfer.class);
            hasMore = Boolean.TRUE.equals(responseTransfer.getMore());
            transfers.add(responseTransfer);
        }
        while (hasMore);

        return transfers;
    }

    public InteractionTransactionalState createTransactionalState(final UnsignedInteger handle)
    {
        return new InteractionTransactionalState(handle);
    }
}
