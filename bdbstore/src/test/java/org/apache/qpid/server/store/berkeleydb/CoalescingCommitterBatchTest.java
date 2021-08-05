package org.apache.qpid.server.store.berkeleydb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class CoalescingCommitterBatchTest extends UnitTestBase
{
    private EnvironmentFacade _environmentFacade;
    private CoalescingCommitter _coalescingCommitter;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));

        _environmentFacade = mock(EnvironmentFacade.class);
        _coalescingCommitter = new CoalescingCommitter("Test", 8, 500, true, _environmentFacade);
        _coalescingCommitter.start();
    }

    @After
    public void tearDown()
    {
        if (_coalescingCommitter != null)
        {
            _coalescingCommitter.stop();
        }
    }

    @Test
    public void testCommitterEnvironmentFacadeInteractionsOnSyncCommit() throws Exception
    {
        final RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            _coalescingCommitter.commit(null, true);
            fail("Commit should fail");
        }
        catch (RuntimeException e)
        {
            assertEquals("Unexpected failure", testFailure, e);
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
        final RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            final ListenableFuture<?> future = _coalescingCommitter.commitAsync(null, null);
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
        final ListenableFuture<?> future = _coalescingCommitter.commitAsync(null, expectedResult);
        final Object result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals("Unexpected result", expectedResult, result);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }
}
