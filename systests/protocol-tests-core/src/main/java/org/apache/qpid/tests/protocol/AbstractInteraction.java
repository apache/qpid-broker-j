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
package org.apache.qpid.tests.protocol;

import static com.google.common.util.concurrent.Futures.allAsList;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;

public abstract class AbstractInteraction<I extends AbstractInteraction<I>>
{
    private final AbstractFrameTransport<I> _transport;
    private ListenableFuture<?> _latestFuture;
    private Response<?> _latestResponse;

    public AbstractInteraction(final AbstractFrameTransport<I> frameTransport)
    {
        _transport = frameTransport;
    }

    public I consumeResponse(final Class<?>... responseTypes) throws Exception
    {
        final Set<Class<?>> acceptableResponseClasses = new HashSet<>(Arrays.asList(responseTypes));
        return consumeResponse(acceptableResponseClasses);
    }

    protected I consumeResponse(final Set<Class<?>> acceptableResponseClasses) throws Exception
    {
        sync();
        _latestResponse = getNextResponse();
        if ((acceptableResponseClasses.isEmpty() && _latestResponse != null)
            || (acceptableResponseClasses.contains(null) && _latestResponse == null))
        {
            return getInteraction();
        }
        acceptableResponseClasses.remove(null);
        if (_latestResponse != null)
        {
            for (Class<?> acceptableResponseClass : acceptableResponseClasses)
            {
                if (acceptableResponseClass.isAssignableFrom(_latestResponse.getBody().getClass()))
                {
                    return getInteraction();
                }
            }
        }
        throw new IllegalStateException(String.format("Unexpected response. Expected one of '%s' got '%s'.",
                                                      acceptableResponseClasses,
                                                      _latestResponse == null ? null : _latestResponse.getBody()));
    }

    public <T> T consume(final Class<T> expected, final Class<?>... ignore)
            throws Exception
    {
        final Class<?>[] expectedResponses = Arrays.copyOf(ignore, ignore.length + 1);
        expectedResponses[ignore.length] = expected;

        T completed = null;
        do
        {
            Response<?> response = consumeResponse(expectedResponses).getLatestResponse();
            if (expected.isAssignableFrom(response.getBody().getClass()))
            {
                completed = (T) response.getBody();
            }
        }
        while (completed == null);
        return completed;
    }

    protected Response<?> getNextResponse() throws Exception
    {
        return _transport.getNextResponse();
    }

    public I sync() throws InterruptedException, ExecutionException, TimeoutException
    {
        _transport.flush();
        if (_latestFuture != null)
        {
            _latestFuture.get(AbstractFrameTransport.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
            _latestFuture = null;
        }
        return getInteraction();
    }

    public Response<?> getLatestResponse()
    {
        return _latestResponse;
    }

    public <T> T getLatestResponse(Class<T> type)
    {
        if (_latestResponse.getBody() == null)
        {
            throw new IllegalStateException(String.format("Unexpected response. Expected '%s' got '%s'.",
                                                          type.getSimpleName(),
                                                          _latestResponse.getClass()));
        }

        if (!type.isAssignableFrom(_latestResponse.getBody().getClass()))
        {
            throw new IllegalStateException(String.format("Unexpected response. Expected '%s' got '%s'.",
                                                          type.getSimpleName(),
                                                          _latestResponse.getBody()));
        }

        return (T) _latestResponse.getBody();
    }

    protected ListenableFuture<Void> sendPerformativeAndChainFuture(final Object frameBody) throws Exception
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
        return future;
    }

    public I negotiateProtocol() throws Exception
    {
        final ListenableFuture<Void> future = _transport.sendProtocolHeader(getProtocolHeader());
        if (_latestFuture != null)
        {
            _latestFuture = allAsList(_latestFuture, future);
        }
        else
        {
            _latestFuture = future;
        }
        return getInteraction();
    }

    protected AbstractFrameTransport getTransport()
    {
        return _transport;
    }

    public abstract I protocolHeader(final byte[] header);

    protected abstract byte[] getProtocolHeader();

    private I getInteraction()
    {
        return (I) this;
    }
}
