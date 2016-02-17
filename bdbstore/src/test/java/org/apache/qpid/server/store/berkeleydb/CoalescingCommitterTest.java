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
package org.apache.qpid.server.store.berkeleydb;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.test.utils.QpidTestCase;


public class CoalescingCommitterTest extends QpidTestCase
{
    private EnvironmentFacade _environmentFacade;
    private CoalescingCommiter _coalescingCommitter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _environmentFacade = mock(EnvironmentFacade.class);
        _coalescingCommitter = new CoalescingCommiter("Test", _environmentFacade);
        _coalescingCommitter.start();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _coalescingCommitter.stop();
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testCommitterEnvironmentFacadeInteractionsOnSyncCommit() throws Exception
    {
        RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            _coalescingCommitter.commit(null, true);
            fail("Commit should fail");
        }
        catch(RuntimeException e)
        {
            assertEquals("Unexpected failure", testFailure, e);
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        _coalescingCommitter.commit(null, true);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    public void testCommitterEnvironmentFacadeInteractionsOnAsyncCommit() throws Exception
    {
        RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            ListenableFuture<?> future =  _coalescingCommitter.commitAsync(null, null);
            future.get(1000, TimeUnit.MILLISECONDS);
            fail("Async commit should fail");
        }
        catch (ExecutionException e)
        {
            assertEquals("Unexpected failure", testFailure, e.getCause());
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        final String expectedResult = "Test";
        ListenableFuture<?> future =  _coalescingCommitter.commitAsync(null, expectedResult);
        Object result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals("Unexpected result", expectedResult, result);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }
}