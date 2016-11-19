/* Licensed to the Apache Software Foundation (ASF) under one
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
 */
package org.apache.qpid.client;

import static org.apache.qpid.transport.Option.BATCH;
import static org.apache.qpid.transport.Option.NONE;
import static org.apache.qpid.transport.Option.SYNC;
import static org.apache.qpid.transport.Option.UNRELIABLE;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.AMQException;
import org.apache.qpid.client.AMQDestination.Binding;
import org.apache.qpid.client.AMQDestination.DestSyntax;
import org.apache.qpid.client.failover.FailoverException;
import org.apache.qpid.client.failover.FailoverNoopSupport;
import org.apache.qpid.client.failover.FailoverProtectedOperation;
import org.apache.qpid.client.message.AMQMessageDelegateFactory;
import org.apache.qpid.client.message.UnprocessedMessage_0_10;
import org.apache.qpid.client.messaging.address.AddressHelper;
import org.apache.qpid.client.messaging.address.Link;
import org.apache.qpid.client.messaging.address.Link.SubscriptionQueue;
import org.apache.qpid.client.messaging.address.Node;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.transport.*;
import org.apache.qpid.util.Serial;
import org.apache.qpid.util.Strings;

/**
 * This is a 0.10 Session
 */
public class AMQSession_0_10 extends AMQSession<BasicMessageConsumer_0_10, BasicMessageProducer_0_10>
    implements SessionListener
{

    /**
     * This class logger
     */
    private static final Logger _logger = LoggerFactory.getLogger(AMQSession_0_10.class);

    private final String _name;

    private static class Flusher implements Runnable
    {

        private WeakReference<AMQSession_0_10> session;
        private ScheduledFuture<?> _future;

        public Flusher(AMQSession_0_10 session)
        {
            this.session = new WeakReference<AMQSession_0_10>(session);
        }

        public void setFuture(final ScheduledFuture<?> future)
        {
            _future = future;
        }

        public void run()
        {
            AMQSession_0_10 ssn = session.get();
            if (ssn == null)
            {
                if(_future != null)
                {
                    _future.cancel(false);
                }
            }
            else
            {
                try
                {
                    ssn.flushAcknowledgments(true);
                }
                catch (Exception t)
                {
                    _logger.error("error flushing acks", t);
                }
            }
        }
    }


    /**
     * The underlying QpidSession
     */
    private Session _qpidSession;

    /**
     * The latest qpid Exception that has been raised.
     */
    private Object _currentExceptionLock = new Object();
    private QpidException _currentException;

    // a ref on the qpid connection
    private org.apache.qpid.transport.Connection _qpidConnection;

    private long maxAckDelay = Long.getLong("qpid.session.max_ack_delay", 1000);
    private ScheduledFuture<?> _flushTaskFuture = null;
    private RangeSet unacked = RangeSetFactory.createRangeSet();
    private int unackedCount = 0;

    /**
     * Used to store the range of in tx messages
     */
    private final RangeSet _txRangeSet = RangeSetFactory.createRangeSet();
    private int _txSize = 0;
    private boolean _isHardError = Boolean.getBoolean("qpid.session.legacy_exception_behaviour");
    //--- constructors


    /**
     * Creates a new session on a connection.
     *
     * @param con                     The connection on which to create the session.
     * @param channelId               The unique identifier for the session.
     * @param transacted              Indicates whether or not the session is transactional.
     * @param acknowledgeMode         The acknowledgement mode for the session.
     * @param defaultPrefetchHighMark The maximum number of messages to prefetched before suspending the session.
     * @param defaultPrefetchLowMark  The number of prefetched messages at which to resume the session.
     * @param qpidConnection          The qpid connection
     */
    AMQSession_0_10(org.apache.qpid.transport.Connection qpidConnection, AMQConnection con, int channelId,
                    boolean transacted, int acknowledgeMode, int defaultPrefetchHighMark, int defaultPrefetchLowMark,String name)
    {

        super(con, channelId, transacted, acknowledgeMode, defaultPrefetchHighMark,
              defaultPrefetchLowMark);
        _qpidConnection = qpidConnection;
        _name = name;
        _qpidSession = createSession();

        if (maxAckDelay > 0)
        {
            Flusher flusher = new Flusher(this);
            _flushTaskFuture = con.scheduleTask(flusher, 0, maxAckDelay, TimeUnit.MILLISECONDS);
            flusher.setFuture(_flushTaskFuture);
        }
    }

    protected Session createSession()
    {
        Session qpidSession;
        if (_name == null)
        {
            qpidSession = _qpidConnection.createSession(1);
        }
        else
        {
            qpidSession = _qpidConnection.createSession(_name,1);
        }
        if (isTransacted())
        {
            qpidSession.txSelect();
            qpidSession.setTransacted(true);
        }
        qpidSession.setSessionListener(this);

        return qpidSession;
    }

    private void addUnacked(int id)
    {
        synchronized (unacked)
        {
            unacked.add(id);
            unackedCount++;
        }
    }

    private void clearUnacked()
    {
        synchronized (unacked)
        {
            unacked.clear();
            unackedCount = 0;
        }
    }

    protected Connection getQpidConnection()
    {
        return _qpidConnection;
    }

    //------- overwritten methods of class AMQSession

    void failoverPrep()
    {
        syncDispatchQueue(true);
        clearUnacked();
    }

    /**
     * Acknowledge one or many messages.
     *
     * @param deliveryTag The tag of the last message to be acknowledged.
     * @param multiple    <tt>true</tt> to acknowledge all messages up to and including the one specified by the
     *                    delivery tag, <tt>false</tt> to just acknowledge that message.
     */

    public void acknowledgeMessage(long deliveryTag, boolean multiple)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Sending ack for delivery tag " + deliveryTag + " on session " + getChannelId());
        }
        // acknowledge this message
        if (multiple)
        {
            for (Long messageTag : getUnacknowledgedMessageTags())
            {
                if( messageTag <= deliveryTag )
                {
                    addUnacked(messageTag.intValue());
                    getUnacknowledgedMessageTags().remove(messageTag);
                }
            }
            //empty the list of unack messages

        }
        else
        {
            addUnacked((int) deliveryTag);
            getUnacknowledgedMessageTags().remove(deliveryTag);
        }

        long prefetch = getAMQConnection().getMaxPrefetch();

        if (unackedCount >= prefetch/2 || maxAckDelay <= 0 || getAcknowledgeMode() == javax.jms.Session.AUTO_ACKNOWLEDGE)
        {
            flushAcknowledgments();
        }
    }

    protected void flushAcknowledgments()
    {
        flushAcknowledgments(false);
    }

    void flushAcknowledgments(boolean setSyncBit)
    {
        synchronized (unacked)
        {
            if (unackedCount > 0)
            {
                messageAcknowledge
                    (unacked, getAcknowledgeMode() != org.apache.qpid.jms.Session.NO_ACKNOWLEDGE,setSyncBit);
                clearUnacked();
            }
        }
    }

    void messageAcknowledge(final RangeSet ranges, final boolean accept)
    {
        messageAcknowledge(ranges,accept,false);
    }

    void messageAcknowledge(final RangeSet ranges, final boolean accept, final boolean setSyncBit)
    {
        final Session ssn = getQpidSession();
        flushProcessed(ranges,accept);
        if (accept)
        {
            ssn.messageAccept(ranges, UNRELIABLE, setSyncBit ? SYNC : NONE);
        }
    }

    /**
     * Flush any outstanding commands. This causes session complete to be sent.
     * @param ranges the range of command ids.
     * @param batch true if batched.
     */
    void flushProcessed(final RangeSet ranges, final boolean batch)
    {
        final Session ssn = getQpidSession();
        for (final Range range : ranges)
        {
            ssn.processed(range);
        }
        ssn.flushProcessed(batch ? BATCH : NONE);
    }

    /**
     * Bind a queue with an exchange.
     *
     * @param queueName    Specifies the name of the queue to bind. If the queue name is empty,
     *                     refers to the current
     *                     queue for the session, which is the last declared queue.
     * @param exchangeName The exchange name.
     * @param routingKey   Specifies the routing key for the binding.
     * @param arguments    0_8 specific
     */
    @Override
    public void sendQueueBind(final String queueName, final String routingKey,
                              final Map<String,Object> arguments, final String exchangeName,
                              final AMQDestination destination, final boolean nowait)
            throws QpidException
    {
        if (destination == null || destination.getDestSyntax() == DestSyntax.BURL)
        {
            if(destination != null)
            {
                for (String rk: destination.getBindingKeys())
                {
                    doSendQueueBind(queueName, exchangeName, arguments, rk);
                }
                if(!Arrays.asList(destination.getBindingKeys()).contains(routingKey))
                {
                    doSendQueueBind(queueName, exchangeName, arguments, routingKey);
                }
            }
            else
            {
                doSendQueueBind(queueName, exchangeName, arguments, routingKey);
            }
        }
        else
        {
            // Leaving this here to ensure the public method bindQueue in AMQSession.java works as expected.
            List<Binding> bindings = new ArrayList<Binding>();
            bindings.addAll(destination.getNode().getBindings());

            String defaultExchange = destination.getAddressType() == AMQDestination.TOPIC_TYPE ?
                                     destination.getAddressName(): "amq.topic";

            for (Binding binding: bindings)
            {
                // Currently there is a bug (QPID-3317) with setting up and tearing down x-bindings for link.
                // The null check below is a way to side step that issue while fixing QPID-4146
                // Note this issue only affects producers.
                if (binding.getQueue() == null && queueName == null)
                {
                    continue;
                }
                String queue = binding.getQueue() == null ? queueName : binding.getQueue();

                String exchange = binding.getExchange() == null ? defaultExchange : binding.getExchange();

                _logger.debug("Binding queue : " + queue +
                              " exchange: " + exchange +
                              " using binding key " + binding.getBindingKey() +
                              " with args " + Strings.printMap(binding.getArgs()));
                doBind(destination, binding, queue, exchange);
            }
        }

        if (!nowait)
        {
            // We need to sync so that we get notify of an error.
            sync();
        }
    }

    private void doSendQueueBind(final String queueName,
                                 final String exchangeName,
                                 final Map args,
                                 final String rk)
    {
        _logger.debug("Binding queue : " + queueName +
                      " exchange: " + exchangeName +
                      " using binding key " + rk);
        getQpidSession().exchangeBind(queueName,
                                      exchangeName,
                                      rk,
                                      args);
    }


    /**
     * Close this session.
     *
     * @param timeout no used / 0_8 specific
     * @throws QpidException
     * @throws FailoverException
     */
    public void sendClose(long timeout) throws QpidException, FailoverException
    {
        cancelTimerTask();
        flushAcknowledgments();
        try
        {
	        getQpidSession().sync();
	        getQpidSession().close();
        }
        catch (SessionException se)
        {
            setCurrentException(se);
        }

        QpidException amqe = getCurrentException();
        if (amqe != null)
        {
            throw amqe;
        }
    }

    /**
     * Create a queue with a given name.
     *
     * @param name       The queue name
     * @param autoDelete If this field is set and the exclusive field is also set,
     *                   then the queue is deleted when the connection closes.
     * @param durable    If set when creating a new queue,
     *                   the queue will be marked as durable.
     * @param exclusive  Exclusive queues can only be used from one connection at a time.
     * @param arguments  Exclusive queues can only be used from one connection at a time.
     * @throws QpidException
     * @throws FailoverException
     */
    public void sendCreateQueue(String name, final boolean autoDelete, final boolean durable,
                                final boolean exclusive, Map<String, Object> arguments) throws QpidException, FailoverException
    {
        getQpidSession().queueDeclare(name, null, arguments, durable ? Option.DURABLE : Option.NONE,
                                      autoDelete ? Option.AUTO_DELETE : Option.NONE,
                                      exclusive ? Option.EXCLUSIVE : Option.NONE);
        // We need to sync so that we get notify of an error.
        sync();
    }

    /**
     * This method asks the broker to redeliver all unacknowledged messages
     *
     * @throws QpidException
     * @throws FailoverException
     */
    public void sendRecover() throws QpidException, FailoverException
    {
        // release all unacked messages
        RangeSet all = RangeSetFactory.createRangeSet();
        RangeSet delivered = gatherRangeSet(getUnacknowledgedMessageTags());
        RangeSet prefetched = gatherRangeSet(getPrefetchedMessageTags());
        for (Iterator<Range> deliveredIter = delivered.iterator(); deliveredIter.hasNext();)
        {
            Range range = deliveredIter.next();
            all.add(range);
        }
        for (Iterator<Range> prefetchedIter = prefetched.iterator(); prefetchedIter.hasNext();)
        {
            Range range = prefetchedIter.next();
            all.add(range);
        }
        flushProcessed(all, false);
        getQpidSession().messageRelease(delivered, Option.SET_REDELIVERED);
        getQpidSession().messageRelease(prefetched);

        // We need to sync so that we get notify of an error.
        sync();
    }

    private RangeSet gatherRangeSet(ConcurrentLinkedQueue<Long> messageTags)
    {
        RangeSet ranges = RangeSetFactory.createRangeSet();
        while (true)
        {
            Long tag = messageTags.poll();
            if (tag == null)
            {
                break;
            }

            ranges.add(tag.intValue());
        }

        return ranges;
    }

    public void releaseForRollback()
    {
        if (_txSize > 0)
        {
            flushProcessed(_txRangeSet, false);
            getQpidSession().messageRelease(_txRangeSet, Option.SET_REDELIVERED);
            _txRangeSet.clear();
            _txSize = 0;
        }
    }

    /**
     * Release (0_8 notion of Reject) an acquired message
     *
     * @param deliveryTag the message ID
     * @param requeue     always true
     */
    public void rejectMessage(long deliveryTag, boolean requeue)
    {
        // The value of requeue is always true
        RangeSet ranges = RangeSetFactory.createRangeSet();
        ranges.add((int) deliveryTag);
        flushProcessed(ranges, false);
        if (requeue)
        {
            getQpidSession().messageRelease(ranges);
        }
        else
        {
            getQpidSession().messageRelease(ranges, Option.SET_REDELIVERED);
        }
        //I don't think we need to sync
    }

    /**
     * Create an 0_10 message consumer
     */
    public BasicMessageConsumer_0_10 createMessageConsumer(final AMQDestination destination, final int prefetchHigh,
                                                      final int prefetchLow, final boolean noLocal,
                                                      final boolean exclusive, String messageSelector,
                                                      final Map<String,Object> rawSelector, final boolean noConsume,
                                                      final boolean autoClose) throws JMSException
    {
        return new BasicMessageConsumer_0_10(getChannelId(), getAMQConnection(), destination, messageSelector, noLocal,
                getMessageFactoryRegistry(), this, rawSelector, prefetchHigh, prefetchLow,
                                             exclusive, getAcknowledgeMode(), noConsume, autoClose);
    }

    /**
     * Bind a queue with an exchange.
     */

    public boolean isQueueBound(final String exchangeName, final String queueName, final String routingKey)
    {
        return isQueueBound(exchangeName,queueName,routingKey,(Map<String,Object>)null);
    }

    public boolean isQueueBound(final AMQDestination destination)
    {
        return isQueueBound(destination.getExchangeName(),destination.getAMQQueueName(),destination.getRoutingKey(),destination.getBindingKeys());
    }

    public boolean isQueueBound(final String exchangeName, final String queueName, final String routingKey, String[] bindingKeys)
    {
        String rk = null;
        if (bindingKeys != null && bindingKeys.length>0)
        {
            rk = bindingKeys[0];
        }
        else if (routingKey != null)
        {
            rk = routingKey;
        }

        return isQueueBound(exchangeName, queueName, rk, (Map<String,Object>)null);
    }

    public boolean isQueueBound(final String exchangeName, final String queueName, final String bindingKey,Map<String,Object> args)
    {
        boolean res;
        ExchangeBoundResult bindingQueryResult =
            getQpidSession().exchangeBound(exchangeName,queueName, bindingKey, args).get();

        if (bindingKey == null)
        {
            res = !(bindingQueryResult.getExchangeNotFound() || bindingQueryResult.getQueueNotFound());
        }
        else
        {
            if (args == null)
            {
                res = !(bindingQueryResult.getExchangeNotFound() || bindingQueryResult.getKeyNotMatched() || bindingQueryResult.getQueueNotFound() || bindingQueryResult
                        .getQueueNotMatched());
            }
            else
            {
                res = !(bindingQueryResult.getExchangeNotFound() || bindingQueryResult.getKeyNotMatched() || bindingQueryResult.getQueueNotFound() || bindingQueryResult
                        .getQueueNotMatched() || bindingQueryResult.getArgsNotMatched());
            }
        }
        return res;
    }

    @Override
    protected boolean isBound(String exchangeName, String amqQueueName, String routingKey)
    {
        return isQueueBound(exchangeName, amqQueueName, routingKey);
    }

    /**
     * This method is invoked when a consumer is created
     * Registers the consumer with the broker
     */
    public void sendConsume(BasicMessageConsumer_0_10 consumer, String queueName,
                            boolean nowait, int tag)
            throws QpidException, FailoverException
    {
        queueName = preprocessAddressTopic(consumer, queueName);
        boolean preAcquire = consumer.isPreAcquire();

        AMQDestination destination = consumer.getDestination();
        long capacity = consumer.getCapacity();

        Map<String, Object> arguments = consumer.getArguments();

        Link link = destination.getLink();
        if (link != null && link.getSubscription() != null && link.getSubscription().getArgs() != null)
        {
            arguments.putAll(link.getSubscription().getArgs());
        }

        boolean acceptModeNone = getAcknowledgeMode() == NO_ACKNOWLEDGE;

        String queue = queueName == null ? destination.getAddressName() : queueName;
        getQpidSession().messageSubscribe
            (queue, String.valueOf(tag),
             acceptModeNone ? MessageAcceptMode.NONE : MessageAcceptMode.EXPLICIT,
             preAcquire ? MessageAcquireMode.PRE_ACQUIRED : MessageAcquireMode.NOT_ACQUIRED, null, 0, arguments,
             consumer.isExclusive() ? Option.EXCLUSIVE : Option.NONE);

        String consumerTag = (consumer).getConsumerTagString();

        if (capacity == 0)
        {
            getQpidSession().messageSetFlowMode(consumerTag, MessageFlowMode.CREDIT);
        }
        else
        {
            getQpidSession().messageSetFlowMode(consumerTag, MessageFlowMode.WINDOW);
        }
        getQpidSession().messageFlow(consumerTag, MessageCreditUnit.BYTE, 0xFFFFFFFF,
                                     Option.UNRELIABLE);

        if(capacity > 0 && getDispatcher() != null && (isStarted() || isImmediatePrefetch()))
        {
            // set the flow
            getQpidSession().messageFlow(consumerTag,
                                         MessageCreditUnit.MESSAGE,
                                         capacity,
                                         Option.UNRELIABLE);
        }
        sync();
    }

    /**
     * Create an 0_10 message producer
     */
    public BasicMessageProducer_0_10 createMessageProducer(final Destination destination, final Boolean mandatory,
                                                           final Boolean immediate, final long producerId) throws JMSException
    {
        try
        {
            return new BasicMessageProducer_0_10(getAMQConnection(), (AMQDestination) destination, isTransacted(), getChannelId(), this,
                                             producerId, immediate, mandatory);
        }
        catch (QpidException e)
        {
            throw toJMSException("Error creating producer",e);
        }
        catch(TransportException e)
        {
            throw toJMSException("Exception while creating message producer:" + e.getMessage(), e);
        }

    }

    /**
     * creates an exchange if it does not already exist
     */
    public void sendExchangeDeclare(final String name, final String type, final boolean nowait,
                                    boolean durable, boolean autoDelete, boolean internal) throws QpidException, FailoverException
    {
        //The 'internal' parameter is ignored on the 0-10 path, the protocol does not support it
        sendExchangeDeclare(name, type, null, null, nowait, durable, autoDelete);
    }

    public void sendExchangeDeclare(final String name, final String type, final boolean nowait,
                                    boolean durable, boolean autoDelete, Map<String,Object> arguments, final boolean passive) throws
                                                                                                                              QpidException, FailoverException
    {
        sendExchangeDeclare(name, type, null,
                            arguments,
                            nowait, durable, autoDelete);
    }


    public void sendExchangeDeclare(final String name, final String type,
            final String alternateExchange, final Map<String, Object> args,
            final boolean nowait, boolean durable, boolean autoDelete) throws QpidException
    {
        getQpidSession().exchangeDeclare(
                name,
                type,
                alternateExchange,
                args,
                name.startsWith("amq.") ? Option.PASSIVE : Option.NONE,
                durable ? Option.DURABLE : Option.NONE,
                autoDelete ? Option.AUTO_DELETE : Option.NONE);
        // We need to sync so that we get notify of an error.
        if (!nowait)
        {
            sync();
        }
    }

    /**
     * deletes an exchange
     */
    public void sendExchangeDelete(final String name, final boolean nowait)
                throws QpidException, FailoverException
    {
        getQpidSession().exchangeDelete(name);
        // We need to sync so that we get notify of an error.
        if (!nowait)
        {
            sync();
        }
    }

    /**
     * Declare a queue with the given queueName
     */
    public String send0_10QueueDeclare(final AMQDestination amqd, final boolean noLocal,
                                       final boolean nowait, boolean passive)
            throws QpidException
    {
        String queueName;
        if (amqd.getAMQQueueName() == null)
        {
            // generate a name for this queue
            queueName = createTemporaryQueueName();
            amqd.setQueueName(queueName);
        }
        else
        {
            queueName = amqd.getAMQQueueName();
        }

        if (amqd.getDestSyntax() == DestSyntax.BURL)
        {
            Map<String,Object> arguments = new HashMap<String,Object>();
            if (noLocal)
            {
                arguments.put(AddressHelper.NO_LOCAL, true);
            }

            getQpidSession().queueDeclare(queueName, "" , arguments,
                                          amqd.isAutoDelete() ? Option.AUTO_DELETE : Option.NONE,
                                          amqd.isDurable() ? Option.DURABLE : Option.NONE,
                                          amqd.isExclusive() ? Option.EXCLUSIVE : Option.NONE,
                                          passive ? Option.PASSIVE : Option.NONE);
        }
        else
        {
            // This code is here to ensure address based destination work with the declareQueue public method in AMQSession.java
            Node node = amqd.getNode();
            Map<String,Object> arguments = new HashMap<String,Object>();
            arguments.putAll(node.getDeclareArgs());
            if (arguments.get(AddressHelper.NO_LOCAL) == null)
            {
                arguments.put(AddressHelper.NO_LOCAL, noLocal);
            }
            getQpidSession().queueDeclare(queueName, node.getAlternateExchange() ,
                    arguments,
                    node.isAutoDelete() ? Option.AUTO_DELETE : Option.NONE,
                    node.isDurable() ? Option.DURABLE : Option.NONE,
                    node.isExclusive() ? Option.EXCLUSIVE : Option.NONE);
        }

        // passive --> false
        if (!nowait)
        {
            // We need to sync so that we get notify of an error.
            sync();
        }
        return queueName;
    }

    /**
     * deletes a queue
     */
    public void sendQueueDelete(final String queueName) throws QpidException, FailoverException
    {
        getQpidSession().queueDelete(queueName);
        // We need to sync so that we get notify of an error.
        sync();
    }

    /**
     * Activate/deactivate the message flow for all the consumers of this session.
     */
    public void sendSuspendChannel(boolean suspend) throws QpidException, FailoverException
    {
        if (suspend)
        {
            for (BasicMessageConsumer consumer : getConsumers())
            {
                getQpidSession().messageStop(String.valueOf(consumer.getConsumerTag()),
                                             Option.UNRELIABLE);
            }
            sync();
        }
        else
        {
            for (BasicMessageConsumer_0_10 consumer : getConsumers())
            {
                String consumerTag = String.valueOf(consumer.getConsumerTag());
                //only set if msg list is null
                try
                {
                    long capacity = consumer.getCapacity();

                    if (capacity == 0)
                    {
                        if (consumer.getMessageListener() != null)
                        {
                            getQpidSession().messageFlow(consumerTag,
                                                         MessageCreditUnit.MESSAGE, 1,
                                                         Option.UNRELIABLE);
                        }
                    }
                    else
                    {
                        getQpidSession()
                            .messageFlow(consumerTag, MessageCreditUnit.MESSAGE,
                                         capacity,
                                         Option.UNRELIABLE);
                    }
                    getQpidSession()
                        .messageFlow(consumerTag, MessageCreditUnit.BYTE, 0xFFFFFFFF,
                                     Option.UNRELIABLE);
                }
                catch (Exception e)
                {
                    throw new AMQException(ErrorCodes.INTERNAL_ERROR, "Error while trying to get the listener", e);
                }
            }
        }
        // We need to sync so that we get notify of an error.
        sync();
    }


    public void sendRollback() throws QpidException, FailoverException
    {
        getQpidSession().txRollback();
        // We need to sync so that we get notify of an error.
        sync();
    }

    //------ Private methods
    /**
     * Access to the underlying Qpid Session
     *
     * @return The associated Qpid Session.
     */
    protected Session getQpidSession()
    {
        return _qpidSession;
    }


    /**
     * Get the latest thrown exception.
     *
     * @throws SessionException get the latest thrown error.
     */
    public QpidException getCurrentException()
    {
        QpidException amqe = null;
        synchronized (_currentExceptionLock)
        {
            if (_currentException != null)
            {
                amqe = _currentException;
                _currentException = null;
            }
        }
        return amqe;
    }

    public void opened(Session ssn) {}

    public void resumed(Session ssn)
    {
        _qpidConnection = ssn.getConnection();
    }

    public void message(Session ssn, MessageTransfer xfr)
    {
        messageReceived(new UnprocessedMessage_0_10(xfr));
    }

    public void exception(Session ssn, SessionException exc)
    {
        setCurrentException(exc);
    }

    public void closed(Session ssn)
    {
        try
        {
            super.closed(null);
            if (_flushTaskFuture != null)
            {
                _flushTaskFuture.cancel(false);
                _flushTaskFuture = null;
            }
        } catch (Exception e)
        {
            _logger.error("Error closing JMS session", e);
        }
    }

    public QpidException getLastException()
    {
        return getCurrentException();
    }

    @Override
    protected String declareQueue(final AMQDestination amqd,
                                          final boolean noLocal, final boolean nowait, final boolean passive)
            throws QpidException
    {
        return new FailoverNoopSupport<String, QpidException>(
                new FailoverProtectedOperation<String, QpidException>()
                {
                    public String execute() throws QpidException, FailoverException
                    {
                        // Generate the queue name if the destination indicates that a client generated name is to be used.
                        if (amqd.isNameRequired())
                        {
                            String binddingKey = "";
                            for(String key : amqd.getBindingKeys())
                            {
                               binddingKey = binddingKey + "_" + key;
                            }
                            amqd.setQueueName(binddingKey + "@"
                                    + amqd.getExchangeName() + "_" + UUID.randomUUID());
                        }
                        return send0_10QueueDeclare(amqd, noLocal, nowait, passive);
                    }
                }, getAMQConnection()).execute();
    }

    protected Long requestQueueDepth(AMQDestination amqd, boolean sync)
    {
        flushAcknowledgments();
        if (sync)
        {
            getQpidSession().sync();
        }
        return getQpidSession().queueQuery(amqd.getQueueName()).get().getMessageCount();
    }


    /**
     * Store non committed messages for this session
     * @param id
     */
    @Override protected void addDeliveredMessage(long id)
    {
        _txRangeSet.add((int) id);
        _txSize++;
    }

    /**
     * With 0.10 messages are consumed with window mode, we must send a completion
     * before the window size is reached so credits don't dry up.
     */
    protected void sendTxCompletionsIfNecessary()
    {
        // this is a heuristic, we may want to have that configurable
        if (_txSize > 0 && (getAMQConnection().getMaxPrefetch() == 1 ||
                getAMQConnection().getMaxPrefetch() != 0 && _txSize % (getAMQConnection().getMaxPrefetch() / 2) == 0))
        {
            // send completed so consumer credits don't dry up
            messageAcknowledge(_txRangeSet, false);
        }
    }

    public void commitImpl() throws QpidException, FailoverException, TransportException
    {
        if( _txSize > 0 )
        {
            messageAcknowledge(_txRangeSet, true);
            _txRangeSet.clear();
            _txSize = 0;
        }

        getQpidSession().setAutoSync(true);
        try
        {
            getQpidSession().txCommit();
        }
        finally
        {
            getQpidSession().setAutoSync(false);
        }
        // We need to sync so that we get notify of an error.
        sync();
    }

    protected final boolean tagLE(long tag1, long tag2)
    {
        return Serial.le((int) tag1, (int) tag2);
    }

    protected final boolean updateRollbackMark(long currentMark, long deliveryTag)
    {
        return Serial.lt((int) currentMark, (int) deliveryTag);
    }

    public void sync() throws QpidException
    {
        try
        {
            getQpidSession().sync();
        }
        catch (SessionException se)
        {
            setCurrentException(se);
        }

        QpidException amqe = getCurrentException();
        if (amqe != null)
        {
            throw amqe;
        }
    }

    public void setCurrentException(SessionException se)
    {
        synchronized (_currentExceptionLock)
        {
            ExecutionException ee = se.getException();
            int code = ErrorCodes.INTERNAL_ERROR;
            if (ee != null)
            {
                code = ee.getErrorCode().getValue();
            }
            QpidException amqe = new AMQException(code, _isHardError, se.getMessage(), se.getCause());
            _currentException = amqe;
        }
        if (!_isHardError)
        {
            cancelTimerTask();
            stopDispatcherThread();
            try
            {
                closed(_currentException);
            }
            catch(Exception e)
            {
                _logger.warn("Error closing session", e);
            }
            getAMQConnection().exceptionReceived(_currentException);
        }
        else
        {
            getAMQConnection().closed(_currentException);
        }
    }

    public AMQMessageDelegateFactory getMessageDelegateFactory()
    {
        return AMQMessageDelegateFactory.FACTORY_0_10;
    }

    @Override
    public boolean isExchangeExist(AMQDestination dest,boolean assertNode) throws QpidException
    {
        boolean match = true;
        ExchangeQueryResult result = getQpidSession().exchangeQuery(dest.getAddressName(), Option.NONE).get();
        match = !result.getNotFound();
        Node node = dest.getNode();

        if (match)
        {
            if (assertNode)
            {
                match =  (result.getDurable() == node.isDurable()) &&
                         (node.getExchangeType() != null &&
                          node.getExchangeType().equals(result.getType())) &&
                         (matchProps(result.getArguments(),node.getDeclareArgs()));
            }
            else
            {
                _logger.debug("Setting Exchange type " + result.getType());
                node.setExchangeType(result.getType());
                dest.setExchangeClass(result.getType());
            }
        }

        if (assertNode)
        {
            if (!match)
            {
                throw new QpidException("Assert failed for address : " + dest  +", Result was : " + result);
            }
        }

        return match;
    }

    @Override
    public boolean isQueueExist(AMQDestination dest, boolean assertNode) throws QpidException
    {
        Node node = dest.getNode();
        return isQueueExist(dest.getAddressName(), assertNode,
                            node.isDurable(), node.isAutoDelete(),
                            node.isExclusive(), node.getDeclareArgs());
    }
    
    public boolean isQueueExist(String queueName, boolean assertNode,
                                boolean durable, boolean autoDelete,
                                boolean exclusive, Map<String, Object> args) throws QpidException
    {    
        boolean match = true;
        try
        {
            QueueQueryResult result = getQpidSession().queueQuery(queueName, Option.NONE).get();
            match = queueName.equals(result.getQueue());

            if (match && assertNode)
            {
                match = (result.getDurable() == durable) &&
                         (result.getAutoDelete() == autoDelete) &&
                         (result.getExclusive() == exclusive) &&
                         (matchProps(result.getArguments(),args));
            }

            if (assertNode)
            {
                if (!match)
                {
                    throw new QpidException("Assert failed for queue : " + queueName  +", Result was : " + result);
                }
            }
        }
        catch(SessionException e)
        {
            final ExecutionException underlying = e.getException();
            if (underlying == null)
            {
                throw new QpidException("Error querying queue", e);
            }
            else
            {
                if (underlying.getErrorCode() == ExecutionErrorCode.RESOURCE_DELETED
                    || underlying.getErrorCode() == ExecutionErrorCode.NOT_FOUND)
                {
                    match = false;
                }
                else
                {
                    throw new AMQException(underlying.getErrorCode().getValue(),
                                           "Error querying queue", e);
                }
            }
        }
        return match;
    }

    private boolean matchProps(Map<String,Object> target,Map<String,Object> source)
    {
        boolean match = true;
        for (String key: source.keySet())
        {
            match = target.containsKey(key) &&
                    (target.get(key).equals(source.get(key))
                    || (target.get(key) instanceof Number)
                        && source.get(key) instanceof Number &&
                        (((Number) target.get(key)).longValue()) == (((Number) source.get(key)).longValue()));

            if (!match)
            {
                StringBuffer buf = new StringBuffer();
                buf.append("Property given in address did not match with the args sent by the broker.");
                buf.append(" Expected { ").append(key).append(" : ").append(source.get(key)).append(" }, ");
                buf.append(" Actual { ").append(key).append(" : ").append(target.get(key)).append(" }");
                _logger.debug(buf.toString());
                return match;
            }
        }

        return match;
    }

    @Override
    public int resolveAddressType(AMQDestination dest) throws QpidException
    {
       int type = dest.getAddressType();
       String name = dest.getAddressName();
       if (type != AMQDestination.UNKNOWN_TYPE)
       {
           return type;
       }
       else
       {
            ExchangeBoundResult result = getQpidSession().exchangeBound(name,name,null,null).get();
            if (result.getQueueNotFound() && result.getExchangeNotFound()) {
                //neither a queue nor an exchange exists with that name; treat it as a queue
                type = AMQDestination.QUEUE_TYPE;
            } else if (result.getExchangeNotFound()) {
                //name refers to a queue
                type = AMQDestination.QUEUE_TYPE;
            } else if (result.getQueueNotFound()) {
                //name refers to an exchange
                type = AMQDestination.TOPIC_TYPE;
            } else {
                //both a queue and exchange exist for that name
                throw new QpidException("Ambiguous address, please specify queue or topic as node type");
            }
            dest.setAddressType(type);
            return type;
        }
    }

    @Override
    void createSubscriptionQueue(AMQDestination dest, boolean noLocal, String messageSelector) throws QpidException
    {
        Link link = dest.getLink();
        String queueName = dest.getQueueName();

        if (queueName == null)
        {
            queueName = link.getName() == null ? "TempQueue" + UUID.randomUUID() : link.getName();
            dest.setQueueName(queueName);
        }

        SubscriptionQueue queueProps = link.getSubscriptionQueue();
        Map<String,Object> arguments = queueProps.getDeclareArgs();
        if (!arguments.containsKey((AddressHelper.NO_LOCAL)))
        {
            arguments.put(AddressHelper.NO_LOCAL, noLocal);
        }

        if (link.isDurable() && queueName.startsWith("TempQueue"))
        {
            throw new QpidException("You cannot mark a subscription queue as durable without providing a name for the link.");
        }

        getQpidSession().queueDeclare(queueName,
                queueProps.getAlternateExchange(), arguments,
                queueProps.isAutoDelete() ? Option.AUTO_DELETE : Option.NONE,
                link.isDurable() ? Option.DURABLE : Option.NONE,
                queueProps.isExclusive() ? Option.EXCLUSIVE : Option.NONE);

        Map<String,Object> bindingArguments = new HashMap<String, Object>();
        bindingArguments.put(AMQPFilterTypes.JMS_SELECTOR.getValue(), messageSelector == null ? "" : messageSelector);
        getQpidSession().exchangeBind(queueName,
                              dest.getAddressName(),
                              dest.getSubject(),
                              bindingArguments);
    }

    protected void acknowledgeImpl()
    {
        RangeSet ranges = gatherRangeSet(getUnacknowledgedMessageTags());

        if(ranges.size() > 0 )
        {
            messageAcknowledge(ranges, true);
            if (getAMQConnection().getSyncClientAck())
            {
                getQpidSession().sync();
            }
        }
    }

    @Override
    void resubscribe() throws QpidException
    {
        // Clear txRangeSet/unacknowledgedMessageTags so we don't complete commands corresponding to
        //messages that came from the old broker.
        _txRangeSet.clear();
        _txSize = 0;

        super.resubscribe();
        getQpidSession().sync();
    }

    @Override
    void stop() throws QpidException
    {
        // Stop the server delivering messages to this session.
        suspendChannelIfNotClosing();
        drainDispatchQueueWithDispatcher();
        stopExistingDispatcher();

        for (BasicMessageConsumer consumer : getConsumers())
        {
            List<Long> tags = consumer.drainReceiverQueueAndRetrieveDeliveryTags();
            getPrefetchedMessageTags().addAll(tags);
        }

        RangeSet delivered = gatherRangeSet(getUnacknowledgedMessageTags());
		RangeSet prefetched = gatherRangeSet(getPrefetchedMessageTags());
		RangeSet all = RangeSetFactory.createRangeSet(delivered.size()
					+ prefetched.size());

		for (Iterator<Range> deliveredIter = delivered.iterator(); deliveredIter.hasNext();)
		{
			Range range = deliveredIter.next();
			all.add(range);
		}

		for (Iterator<Range> prefetchedIter = prefetched.iterator(); prefetchedIter.hasNext();)
		{
			Range range = prefetchedIter.next();
			all.add(range);
		}

		flushProcessed(all, false);
		getQpidSession().messageRelease(delivered,Option.SET_REDELIVERED);
		getQpidSession().messageRelease(prefetched);
		sync();
    }

    @Override
    public boolean isFlowBlocked()
    {
        return _qpidSession.isFlowBlocked();
    }

    @Override
    public void setFlowControl(boolean active)
    {
        // Supported by 0-8..0-9-1 only
        throw new UnsupportedOperationException("Operation not supported by this protocol");
    }

    private void cancelTimerTask()
    {
        if (_flushTaskFuture != null)
        {
            _flushTaskFuture.cancel(false);
            _flushTaskFuture = null;
        }
    }

    @Override
    protected void handleQueueNodeCreation(AMQDestination dest, boolean noLocal) throws QpidException
    {
        Node node = dest.getNode();
        Map<String,Object> arguments = node.getDeclareArgs();
        if (!arguments.containsKey((AddressHelper.NO_LOCAL)))
        {
            arguments.put(AddressHelper.NO_LOCAL, noLocal);
        }
        getQpidSession().queueDeclare(dest.getAddressName(),
                node.getAlternateExchange(), arguments,
                node.isAutoDelete() ? Option.AUTO_DELETE : Option.NONE,
                node.isDurable() ? Option.DURABLE : Option.NONE,
                node.isExclusive() ? Option.EXCLUSIVE : Option.NONE);

        createBindings(dest, dest.getNode().getBindings());
        sync();
    }

    @Override
    void handleExchangeNodeCreation(AMQDestination dest) throws QpidException
    {
        Node node = dest.getNode();
        sendExchangeDeclare(dest.getAddressName(),
                node.getExchangeType(),
                node.getAlternateExchange(),
                node.getDeclareArgs(),
                false,
                node.isDurable(),
                node.isAutoDelete());

        // If bindings are specified without a queue name and is called by the producer,
        // the broker will send an exception as expected.
        createBindings(dest, dest.getNode().getBindings());
        sync();
    }

    protected void doBind(final AMQDestination dest, final Binding binding, final String queue, final String exchange)
    {
        getQpidSession().exchangeBind(queue,
                                      exchange,
                                      binding.getBindingKey(),
                                      binding.getArgs());
    }

    void handleLinkDelete(AMQDestination dest) throws QpidException
    {
        // We need to destroy link bindings
        String defaultExchangeForBinding = dest.getAddressType() == AMQDestination.TOPIC_TYPE ? dest
                .getAddressName() : "amq.topic";

        String defaultQueueName = null;
        if (AMQDestination.QUEUE_TYPE == dest.getAddressType())
        {
            defaultQueueName = dest.getQueueName();
        }
        else
        {
            defaultQueueName = dest.getLink().getName() != null ? dest.getLink().getName() : dest.getQueueName();
        }

        for (Binding binding: dest.getLink().getBindings())
        {
            String queue = binding.getQueue() == null?
                    defaultQueueName: binding.getQueue();

            String exchange = binding.getExchange() == null ?
                        defaultExchangeForBinding :
                        binding.getExchange();

            if (_logger.isDebugEnabled())
            {
                _logger.debug("Unbinding queue : " + queue +
                         " exchange: " + exchange +
                         " using binding key " + binding.getBindingKey() +
                         " with args " + Strings.printMap(binding.getArgs()));
            }
            getQpidSession().exchangeUnbind(queue, exchange,
                                            binding.getBindingKey());
        }
    }

    void deleteSubscriptionQueue(AMQDestination dest) throws QpidException
    {
        // We need to delete the subscription queue.
        if (dest.getAddressType() == AMQDestination.TOPIC_TYPE &&
            dest.getLink().getSubscriptionQueue().isExclusive() &&
            isQueueExist(dest.getQueueName(), false, false, false, false, null))
        {
            getQpidSession().queueDelete(dest.getQueueName());
        }
    }

    @Override
    void handleNodeDelete(AMQDestination dest) throws QpidException
    {
        if (AMQDestination.TOPIC_TYPE == dest.getAddressType())
        {
            if (isExchangeExist(dest,false))
            {
                getQpidSession().exchangeDelete(dest.getAddressName());
                setUnresolved(dest);
            }
        }
        else
        {
            if (isQueueExist(dest,false))
            {
                getQpidSession().queueDelete(dest.getAddressName());
                setUnresolved(dest);
            }
        }
    }
}
