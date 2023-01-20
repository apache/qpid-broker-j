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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class CoalescingCommitterTest extends UnitTestBase
{
    private EnvironmentFacade _environmentFacade;
    private CoalescingCommiter _coalescingCommitter;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _environmentFacade = mock(EnvironmentFacade.class);
        _coalescingCommitter = new CoalescingCommiter("Test", 8, 500, _environmentFacade);
        _coalescingCommitter.start();
    }

    @AfterEach
    public void tearDown()
    {
        if (_coalescingCommitter != null)
        {
            _coalescingCommitter.stop();
        }
    }

    @Test
    public void testCommitterEnvironmentFacadeInteractionsOnSyncCommit()
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
            assertEquals(testFailure, e, "Unexpected failure");
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        _coalescingCommitter.commit(null, true);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
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
            assertEquals(testFailure, e.getCause(), "Unexpected failure");
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        final String expectedResult = "Test";
        ListenableFuture<?> future =  _coalescingCommitter.commitAsync(null, expectedResult);
        Object result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, result, "Unexpected result");

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }
}
