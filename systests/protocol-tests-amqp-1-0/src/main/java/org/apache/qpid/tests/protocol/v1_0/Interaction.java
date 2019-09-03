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

import static org.apache.qpid.server.security.auth.manager.AbstractScramAuthenticationManager.PLAIN;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.framing.SASLFrame;
import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
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
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.tests.protocol.AbstractInteraction;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class Interaction extends AbstractInteraction<Interaction>
{
    private static final byte[] SASL_AMQP_HEADER_BYTES = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);

    private static final FrameBody EMPTY_FRAME = (channel, conn) -> {
        throw new UnsupportedOperationException();
    };

    private static final SaslFrameBody SASL_EMPTY_FRAME = (channel, conn) -> {
        throw new UnsupportedOperationException();
    };

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
    private final SaslInit _saslInit;
    private final SaslResponse _saslResponse;
    private final BrokerAdmin _brokerAdmin;
    private final BrokerAdmin.PortType _portType;
    private byte[] _protocolHeader;
    private UnsignedShort _connectionChannel;
    private UnsignedShort _sessionChannel;
    private int _deliveryIdCounter;
    private List<Transfer> _latestDelivery;
    private Object _decodedLatestDelivery;
    private UnsignedInteger _latestDeliveryId;
    private Map<String, Object> _latestDeliveryApplicationProperties;
    private Map<Class, FrameBody> _latestResponses = new HashMap<>();
    private AtomicLong _receivedDeliveryCount = new AtomicLong();
    private AtomicLong _coordinatorCredits = new AtomicLong();
    private InteractionTransactionalState _transactionalState;

    Interaction(final FrameTransport frameTransport, final BrokerAdmin brokerAdmin, final BrokerAdmin.PortType portType)
    {
        super(frameTransport);
        _brokerAdmin = brokerAdmin;
        _portType = portType;
        final UnsignedInteger defaultLinkHandle = UnsignedInteger.ZERO;

        _protocolHeader = frameTransport.getProtocolHeader();

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
        _transfer.setDeliveryId(UnsignedInteger.valueOf(_deliveryIdCounter));
        _transfer.setMessageFormat(UnsignedInteger.ZERO);

        _disposition = new Disposition();
        _disposition.setFirst(UnsignedInteger.ZERO);
    }

    public void doCloseConnection() throws Exception
    {
        Close close = new Close();

        sendPerformative(close, UnsignedShort.valueOf((short) 0));
        Response<?> response = consumeResponse().getLatestResponse();
        if (!(response.getBody() instanceof Close))
        {
            throw new IllegalStateException(String.format(
                    "Unexpected response to connection Close. Expected Close got '%s'", response.getBody()));
        }
    }

    /////////////////////////
    // Protocol Negotiation //
    /////////////////////////

    @Override
    public Interaction protocolHeader(byte[] header)
    {
        _protocolHeader = header;
        return this;
    }

    @Override
    protected byte[] getProtocolHeader()
    {
        return _protocolHeader;
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

    public Interaction saslEmptyFrame() throws Exception
    {
        sendPerformative(SASL_EMPTY_FRAME);
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

    public Interaction openIdleTimeOut(final int idleTimeOut)
    {
        _open.setIdleTimeOut(UnsignedInteger.valueOf(idleTimeOut));
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


    public Interaction attachSourceFilter(final Map<Symbol, Filter> filters)
    {
        Source source = ((Source) _attach.getSource());
        source.setFilter(filters);
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

    public Interaction detachHandle(UnsignedInteger handle)
    {
        _detach.setHandle(handle);
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

    public Interaction flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
    {
        final Begin begin = getCachedResponse(Begin.class);
        return flowNextIncomingId(begin.getNextOutgoingId().add(UnsignedInteger.valueOf(_receivedDeliveryCount.get())));
    }

    <T extends FrameBody> T getCachedResponse(final Class<T> responseClass)
    {
        return (T)_latestResponses.get(responseClass);
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

    public Interaction flowNextOutgoingId()
    {
        _flow.setNextOutgoingId(UnsignedInteger.valueOf(_deliveryIdCounter));
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

    public Interaction flowDeliveryCount()
    {
        _flow.setDeliveryCount(UnsignedInteger.valueOf(_receivedDeliveryCount.get()));
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

    public Interaction transferDeliveryId()
    {
        _transfer.setDeliveryId(getNextDeliveryId());
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

    public Interaction transferTransactionalStateFromCurrentTransaction()
    {
        return transferTransactionalState(getCurrentTransactionId());
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

    public Interaction transferPayload(final QpidByteBuffer payload)
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
        try (QpidByteBuffer encodedForm = section.getEncodedForm())
        {
            transfer.setPayload(encodedForm);
        }
        finally
        {
            section.dispose();
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
        try (QpidByteBuffer payload = transfer.getPayload())
        {
            if (payload != null)
            {
                transferCopy.setPayload(payload);
            }
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

    public Interaction dispositionTransactionalStateFromCurrentTransaction(final Outcome outcome)
    {
        return dispositionTransactionalState(getCurrentTransactionId(), outcome);
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


    public Interaction dispositionLast(final UnsignedInteger last)
    {
        _disposition.setLast(last);
        return this;
    }

    public Interaction dispositionFirstFromLatestDelivery()
    {
        _disposition.setFirst(_latestDeliveryId);
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


    public UnsignedInteger getCoordinatorHandle()
    {
        return _transactionalState == null ? null : _transactionalState.getHandle();
    }

    public Binary getCurrentTransactionId()
    {
        return _transactionalState == null ? null : _transactionalState.getCurrentTransactionId();
    }

    public DeliveryState getCoordinatorLatestDeliveryState()
    {
        return _transactionalState == null ? null : _transactionalState.getDeliveryState();
    }

    public Interaction txnAttachCoordinatorLink(final UnsignedInteger handle) throws Exception
    {
        return txnAttachCoordinatorLink(handle,
                                        this::txDefaultUnexpectedResponseHandler,
                                        Accepted.ACCEPTED_SYMBOL,
                                        Rejected.REJECTED_SYMBOL);
    }

    public Interaction txnAttachCoordinatorLink(final UnsignedInteger handle,
                                                final Consumer<Response<?>> unexpectedResponseHandler) throws Exception
    {
        return txnAttachCoordinatorLink(handle,
                                        unexpectedResponseHandler,
                                        Accepted.ACCEPTED_SYMBOL,
                                        Rejected.REJECTED_SYMBOL);
    }

    public Interaction txnAttachCoordinatorLink(final UnsignedInteger handle,
                                                final Consumer<Response<?>> unexpectedResponseHandler,
                                                final Symbol... outcomes) throws Exception
    {
        Attach attach = new Attach();
        attach.setName("testTransactionCoordinator-" + handle);
        attach.setHandle(handle);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        attach.setTarget(new Coordinator());
        attach.setRole(Role.SENDER);
        Source source = new Source();
        attach.setSource(source);
        source.setOutcomes(outcomes);
        _transactionalState = new InteractionTransactionalState(handle);
        sendPerformativeAndChainFuture(attach, _sessionChannel);

        // attach expected
        Consumer<Response<?>> handler = unexpectedResponseHandler == null
                ? this::txDefaultUnexpectedResponseHandler
                : unexpectedResponseHandler;
        consumeResponse().assertLatestResponse(handler);

        // flow expected
        consumeResponse().assertLatestResponse(handler);

        Flow flow = getLatestResponse(Flow.class);
        _coordinatorCredits.set(flow.getLinkCredit().longValue());
        return this;
    }

    private void txDefaultUnexpectedResponseHandler(Response<?> r)
    {
        if (r == null || r.getBody() == null || !(r.getBody() instanceof Attach || r.getBody() instanceof Flow))
        {
            throw new IllegalStateException(String.format("Could not attach coordinator link. Got %s", r));
        }
    }

    public Interaction txnDeclare() throws Exception
    {
        sendPayloadToCoordinator(new Declare(), _transactionalState.getHandle());
        final DeliveryState state = handleCoordinatorResponse();
        _transactionalState.setDeliveryState(state);
        final Binary transactionId = ((Declared) state).getTxnId();
        _transactionalState.setLastTransactionId(transactionId);
        return this;
    }

    public Interaction txnSendDischarge(final boolean failed)
            throws Exception
    {
        return txnSendDischarge(_transactionalState.getCurrentTransactionId(), failed);
    }

    public Interaction txnSendDischarge(Binary transactionId, final boolean failed)
            throws Exception
    {
        final Discharge discharge = new Discharge();
        discharge.setTxnId(transactionId);
        discharge.setFail(failed);
        sendPayloadToCoordinator(discharge, _transactionalState.getHandle());
        return this;
    }

    public Interaction txnDischarge(boolean failed) throws Exception
    {
        return txnDischarge(_transactionalState.getCurrentTransactionId(), failed);
    }

    public Interaction txnDischarge(Binary transactionId, boolean failed) throws Exception
    {
        txnSendDischarge(transactionId, failed);
        final DeliveryState state = handleCoordinatorResponse();
        _transactionalState.setDeliveryState(state);
        return this;
    }

    private void sendPayloadToCoordinator(final Object payload, final UnsignedInteger handle)
            throws Exception
    {
        final Transfer transfer = createTransactionTransfer(handle);
        transferPayload(transfer, payload);
        sendPerformativeAndChainFuture(transfer, _sessionChannel);
    }

    private DeliveryState handleCoordinatorResponse() throws Exception
    {
        final Set<Class<?>> expected = new HashSet<>(Collections.singletonList(Disposition.class));
        if (_coordinatorCredits.decrementAndGet() == 0)
        {
            expected.add(Flow.class);
        }

        final Map<Class<?>, ?> responses = consumeResponses(expected, Collections.singleton(Flow.class));

        final Disposition disposition = (Disposition) responses.get(Disposition.class);
        if (expected.contains(Flow.class))
        {
            Flow flow = (Flow) responses.get(Flow.class);
            if (flow.getHandle().equals(getCoordinatorHandle()))
            {
                final UnsignedInteger linkCredit = flow.getLinkCredit();
                if (linkCredit != null)
                {
                    _coordinatorCredits.set(linkCredit.longValue());
                }
            }
        }
        if (!Boolean.TRUE.equals(disposition.getSettled()))
        {
            throw new IllegalStateException("Coordinator disposition is not settled");
        }
        return disposition.getState();
    }

    private Map<Class<?>, ?> consumeResponses(final Set<Class<?>> responseTypes, Set<Class<?>> ignore)
            throws Exception
    {
        final Map<Class<?>, Object> results = new HashMap<>();
        final Set<Class<?>> expected = new HashSet<>(responseTypes);
        expected.addAll(ignore);
        do
        {
            Response<?> response = consumeResponse(expected).getLatestResponse();
            if (response != null && response.getBody() instanceof FrameBody)
            {
                Class<?> bodyClass = response.getBody().getClass();
                results.put(bodyClass, response.getBody());
            }
        }
        while (!results.keySet().containsAll(responseTypes));
        return results;
    }

    private Transfer createTransactionTransfer(final UnsignedInteger handle)
    {
        Transfer transfer = new Transfer();
        transfer.setHandle(handle);
        transfer.setDeliveryId(getNextDeliveryId());
        transfer.setDeliveryTag(new Binary(("transaction-"
                                            + transfer.getDeliveryId()).getBytes(StandardCharsets.UTF_8)));
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
        SASLFrame transportFrame = new SASLFrame(frameBody);
        sendPerformativeAndChainFuture(transportFrame);
    }

    private void sendPerformativeAndChainFuture(final FrameBody frameBody, final UnsignedShort channel) throws Exception
    {
        final TransportFrame transportFrame;
        try (QpidByteBuffer payload = frameBody instanceof Transfer ? ((Transfer) frameBody).getPayload() : null)
        {
            final QpidByteBuffer duplicate;
            if (payload == null)
            {
                duplicate = null;
            }
            else
            {
                duplicate = payload.duplicate();
            }
            transportFrame = new TransportFrame(channel.shortValue(), frameBody, duplicate);
            ListenableFuture<Void> listenableFuture = sendPerformativeAndChainFuture(transportFrame);
            if (frameBody instanceof Transfer)
            {
                listenableFuture.addListener(() -> ((Transfer) frameBody).dispose(), MoreExecutors.directExecutor());
            }
            if (duplicate != null)
            {
                listenableFuture.addListener(() -> duplicate.dispose(), MoreExecutors.directExecutor());
            }
        }
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

    public Interaction receiveDelivery(Class<?>... ignore) throws Exception
    {
        sync();
        _latestDelivery = receiveAllTransfers(ignore);
        _latestDeliveryId = _latestDelivery.size() > 0 ? _latestDelivery.get(0).getDeliveryId() : null;
        _receivedDeliveryCount.incrementAndGet();
        return this;
    }

    public UnsignedInteger getLatestDeliveryId()
    {
        return _latestDeliveryId;
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
        _latestDeliveryApplicationProperties = messageDecoder.getApplicationProperties();
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

    public Map<String, Object> getLatestDeliveryApplicationProperties()
    {
        return _latestDeliveryApplicationProperties;
    }

    private List<Transfer> receiveAllTransfers(final Class<?>... ignore) throws Exception
    {
        List<Transfer> transfers = new ArrayList<>();
        boolean hasMore = true;
        do
        {
            Set<Class<?>> responseTypesSet = new HashSet<>(Arrays.asList(ignore));
            responseTypesSet.add(Transfer.class);
            Class<?>[] responseTypes = responseTypesSet.toArray(new Class<?>[responseTypesSet.size()]);
            Response<?> latestResponse = consumeResponse(responseTypes).getLatestResponse();
            if (latestResponse.getBody() instanceof Transfer)
            {
                Transfer responseTransfer = (Transfer) latestResponse.getBody();
                hasMore = Boolean.TRUE.equals(responseTransfer.getMore());
                transfers.add(responseTransfer);
            }
        }
        while (hasMore);

        return transfers;
    }

    ///////////
    // Empty //
    ///////////

    public Interaction emptyFrame() throws Exception
    {
        sendPerformative(EMPTY_FRAME, UnsignedShort.ZERO);
        return this;
    }

    @Override
    protected Response<?> getNextResponse() throws Exception
    {
        Response<?> response = super.getNextResponse();
        if (response instanceof FrameDecodingError)
        {
            throw new IllegalStateException(String.format("Failed to decode response: %s",
                                                          ((FrameDecodingError) response).getError()));
        }
        if (response != null && response.getBody() instanceof FrameBody)
        {
            _latestResponses.put(response.getBody().getClass(), (FrameBody) response.getBody());
        }
        return response;
    }

    public <T> Interaction assertLatestResponse(Class<T> type, Consumer<T> assertion)
    {
        T latestResponse = getLatestResponse(type);
        assertion.accept(latestResponse);
        return this;
    }

    public Interaction assertLatestResponse(Consumer<Response<?>> assertion)
    {
        Response<?> latestResponse = getLatestResponse();
        assertion.accept(latestResponse);
        return this;
    }

    public void detachEndCloseUnconditionally() throws Exception
    {
        detachClose(true).detach().end().close().sync();
    }

    public Interaction closeUnconditionally() throws Exception
    {
        close().sync();
        return this;
    }

    public Interaction negotiateOpen() throws Exception
    {
        sendOpen().consumeResponse(Open.class);
        return this;
    }

    public Interaction sendOpen() throws Exception
    {
        if ((_portType == BrokerAdmin.PortType.ANONYMOUS_AMQP || _portType == BrokerAdmin.PortType.ANONYMOUS_AMQPWS)
            && _brokerAdmin.isAnonymousSupported())
        {
            sendProtocolAndOpen();
        }
        else if (_portType == BrokerAdmin.PortType.AMQP
                 && _brokerAdmin.isSASLSupported()
                 && _brokerAdmin.isSASLMechanismSupported(PLAIN))
        {
            sendSasl();
            protocolHeader(getTransport().getProtocolHeader()).sendProtocolAndOpen();
        }
        else
        {
            throw new IllegalStateException("Only ANONYMOUS or PLAIN authentication currently supported by the tests");
        }
        return this;
    }

    private void sendProtocolAndOpen() throws Exception
    {
        negotiateProtocol().consumeResponse().open();
    }

    private void sendSasl() throws Exception
    {
        final byte[] protocolResponse = protocolHeader(SASL_AMQP_HEADER_BYTES)
                .negotiateProtocol().consumeResponse()
                .getLatestResponse(byte[].class);
        if (!Arrays.equals(SASL_AMQP_HEADER_BYTES, protocolResponse))
        {
            throw new IllegalStateException(String.format(
                    "Unexpected protocol '%s' is reported from broker supporting SASL",
                    StringUtil.toHex(protocolResponse)));
        }

        final SaslMechanisms mechanisms = consumeResponse().getLatestResponse(SaslMechanisms.class);
        final Symbol[] supportedMechanisms = mechanisms.getSaslServerMechanisms();
        if (Arrays.stream(supportedMechanisms).noneMatch(m -> m.toString().equalsIgnoreCase(PLAIN)))
        {
            if (Arrays.stream(supportedMechanisms)
                      .noneMatch(m -> m.toString().equalsIgnoreCase(AnonymousAuthenticationManager.MECHANISM_NAME)))
            {
                throw new IllegalStateException(String.format(
                        "PLAIN or ANONYMOUS SASL mechanism is not listed among supported '%s'",
                        Arrays.stream(supportedMechanisms)
                              .map(String::valueOf)
                              .collect(Collectors.joining(","))));
            }
            else
            {
                authenticate(AnonymousAuthenticationManager.MECHANISM_NAME, new byte[0]);
            }
        }
        else
        {
            byte[] initialResponseBytes =
                    String.format("\0%s\0%s", _brokerAdmin.getValidUsername(), _brokerAdmin.getValidPassword())
                          .getBytes(StandardCharsets.US_ASCII);
            authenticate(PLAIN, initialResponseBytes);
        }
    }

    private void authenticate(final String mechanism, final byte[] initialResponseBytes) throws Exception
    {
        final SaslOutcome saslOutcome = saslMechanism(Symbol.getSymbol(mechanism))
                .saslInitialResponse(new Binary(initialResponseBytes))
                .saslInit().consumeResponse()
                .getLatestResponse(SaslOutcome.class);

        if (!SaslCode.OK.equals(saslOutcome.getCode()))
        {
            throw new IllegalStateException("Authentication failed.");
        }
    }
}
