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
package org.apache.qpid.server.protocol.v0_8;

import java.io.IOException;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverSyncBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRejectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueuePurgeBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ServerDecoder extends AMQDecoder<ServerMethodProcessor<? extends ServerChannelMethodProcessor>>
{
    private final FrameBoundaryValidatingServerMethodProcessor _validatingMethodProcessor;

    /**
     * Creates a new AMQP decoder.
     *
     * @param methodProcessor          method processor
     */
    public ServerDecoder(final ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor)
    {
        this(methodProcessor, AMQPConnection_0_8.DEFAULT_CODEC_MAX_NESTED_OBJECTS);
    }

    ServerDecoder(final ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor,
                  final int maxNestedObjects)
    {
        super(true, methodProcessor, maxNestedObjects);
        _validatingMethodProcessor = new FrameBoundaryValidatingServerMethodProcessor(methodProcessor);
    }

    public void decodeBuffer(final QpidByteBuffer buf)
            throws AMQFrameDecodingException, AMQProtocolVersionException, IOException
    {
        decode(buf);
    }


    @Override
    protected void processMethod(final int channelId,
                                 final QpidByteBuffer in)
            throws AMQFrameDecodingException
    {
        final ServerMethodProcessor<? extends ServerChannelMethodProcessor> actualMethodProcessor =
                getMethodProcessor();
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor = _validatingMethodProcessor;
        _validatingMethodProcessor.begin(in);
        try
        {
            if (in.remaining() < Integer.BYTES)
            {
                throw new AMQFrameDecodingException("Method frame does not contain a class and method identifier");
            }
            final int classAndMethod = in.getInt();
            final int classId = classAndMethod >> 16;
            final int methodId = classAndMethod & 0xFFFF;
            try
            {
                if (classId == ConnectionStartOkBody.CLASS_ID && channelId != 0)
                {
                    throw new AMQFrameDecodingException(ErrorCodes.COMMAND_INVALID, "Connection class method " +
                            methodId + " must use channel 0, received channel " + channelId, null);
                }
                methodProcessor.setCurrentMethod(classId, methodId);
                switch (classAndMethod)
                {
                    //CONNECTION_CLASS:
                    case 0x000a000b:
                        ConnectionStartOkBody.process(in, methodProcessor, getMaxNestedObjects());
                        break;
                    case 0x000a0015:
                        ConnectionSecureOkBody.process(in, methodProcessor);
                        break;
                    case 0x000a001f:
                        ConnectionTuneOkBody.process(in, methodProcessor);
                        break;
                    case 0x000a0028:
                        ConnectionOpenBody.process(in, methodProcessor);
                        break;
                    case 0x000a0032:
                        if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8))
                        {
                            throw newValidatedUnknownMethodException(classId, methodId,
                                    methodProcessor.getProtocolVersion());
                        }
                        else
                        {
                            ConnectionCloseBody.process(in, methodProcessor);
                        }
                        break;
                    case 0x000a0033:
                        if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8))
                        {
                            throw newValidatedUnknownMethodException(classId, methodId,
                                    methodProcessor.getProtocolVersion());
                        }
                        else
                        {
                            methodProcessor.receiveConnectionCloseOk();
                        }
                        break;
                    case 0x000a003c:
                        if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8))
                        {
                            ConnectionCloseBody.process(in, methodProcessor);
                        }
                        else
                        {
                            throw newValidatedUnknownMethodException(classId, methodId,
                                    methodProcessor.getProtocolVersion());
                        }
                        break;
                    case 0x000a003d:
                        if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8))
                        {
                            methodProcessor.receiveConnectionCloseOk();
                        }
                        else
                        {
                            throw newValidatedUnknownMethodException(classId, methodId,
                                    methodProcessor.getProtocolVersion());
                        }
                        break;

                    // CHANNEL_CLASS:

                    case 0x0014000a:
                        ChannelOpenBody.process(channelId, in, methodProcessor);
                        break;
                    case 0x00140014:
                        ChannelFlowBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00140015:
                        ChannelFlowOkBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00140028:
                        ChannelCloseBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00140029:
                        methodProcessor.getChannelMethodProcessor(channelId).receiveChannelCloseOk();
                        break;

                    // ACCESS_CLASS:

                    case 0x001e000a:
                        AccessRequestBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;

                    // EXCHANGE_CLASS:

                    case 0x0028000a:
                        ExchangeDeclareBody.process(in, methodProcessor.getChannelMethodProcessor(channelId),
                                getMaxNestedObjects());
                        break;
                    case 0x00280014:
                        ExchangeDeleteBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00280016:
                        ExchangeBoundBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;


                    // QUEUE_CLASS:

                    case 0x0032000a:
                        QueueDeclareBody.process(in, methodProcessor.getChannelMethodProcessor(channelId),
                                getMaxNestedObjects());
                        break;
                    case 0x00320014:
                        QueueBindBody.process(in, methodProcessor.getChannelMethodProcessor(channelId),
                                getMaxNestedObjects());
                        break;
                    case 0x0032001e:
                        QueuePurgeBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00320028:
                        QueueDeleteBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x00320032:
                        QueueUnbindBody.process(in, methodProcessor.getChannelMethodProcessor(channelId),
                                getMaxNestedObjects());
                        break;


                    // BASIC_CLASS:

                    case 0x003c000a:
                        BasicQosBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0014:
                        BasicConsumeBody.process(in, methodProcessor.getChannelMethodProcessor(channelId),
                                getMaxNestedObjects());
                        break;
                    case 0x003c001e:
                        BasicCancelBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0028:
                        BasicPublishBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0046:
                        BasicGetBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0050:
                        BasicAckBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c005a:
                        BasicRejectBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0064:
                        BasicRecoverBody.process(in, methodProcessor.getProtocolVersion(),
                                methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0066:
                        BasicRecoverSyncBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c006e:
                        BasicRecoverSyncBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;
                    case 0x003c0078:
                        BasicNackBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;

                    // CONFIRM CLASS:

                    case 0x0055000a:
                        ConfirmSelectBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                        break;

                    // TX_CLASS:

                    case 0x005a000a:
                        if(!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk())
                        {
                            methodProcessor.getChannelMethodProcessor(channelId).receiveTxSelect();
                        }
                        break;
                    case 0x005a0014:
                        if(!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk())
                        {
                            methodProcessor.getChannelMethodProcessor(channelId).receiveTxCommit();
                        }
                        break;
                    case 0x005a001e:
                        if(!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk())
                        {
                            methodProcessor.getChannelMethodProcessor(channelId).receiveTxRollback();
                        }
                        break;


                    default:
                        throw newValidatedUnknownMethodException(classId, methodId,
                                methodProcessor.getProtocolVersion());
                }

                if (in.hasRemaining())
                {
                    throw new AMQFrameDecodingException("Method frame body was not fully consumed: classId=" +
                            classId + ", methodId=" + methodId + ", remaining=" + in.remaining());
                }
            }
            catch (AMQValueNestingException e)
            {
                if (isMethodProcessorFailure())
                {
                    throw new ConnectionScopedRuntimeException(e);
                }
                throw AMQFrameDecodingException.forDecodingFailure("Could not decode method " + classId + ':' +
                        methodId, classId, methodId, e);
            }
            catch (AMQFrameDecodingException e)
            {
                throw new AMQFrameDecodingException(e.getErrorCode(), e.getMessage(), classId, methodId, e);
            }
            catch (final ConnectionScopedRuntimeException | ServerScopedRuntimeException e)
            {
                throw e;
            }
            catch (final RuntimeException e)
            {
                if (isMethodProcessorFailure())
                {
                    throw e;
                }
                throw AMQFrameDecodingException.forDecodingFailure("Could not decode method " + classId + ':' +
                        methodId, classId, methodId, e);
            }
        }
        finally
        {
            try
            {
                actualMethodProcessor.setCurrentMethod(0, 0);
            }
            finally
            {
                _validatingMethodProcessor.end();
            }
        }
    }

    private boolean isMethodProcessorFailure()
    {
        return _validatingMethodProcessor.isMethodBodyDecoded();
    }

    private AMQFrameDecodingException newValidatedUnknownMethodException(final int classId,
                                                                         final int methodId,
                                                                         final ProtocolVersion protocolVersion)
    {
        _validatingMethodProcessor.publishCurrentMethodIfFrameBodyConsumed();
        return newUnknownMethodException(classId, methodId, protocolVersion);
    }
}
