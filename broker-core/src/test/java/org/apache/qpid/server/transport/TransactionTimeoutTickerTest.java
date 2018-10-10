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

package org.apache.qpid.server.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.qpid.server.util.Action;
import org.apache.qpid.test.utils.UnitTestBase;

public class TransactionTimeoutTickerTest extends UnitTestBase
{

    private TransactionTimeoutTicker _ticker;
    private Supplier<Long> _dateSupplier = mock(Supplier.class);
    private Action<Long> _notificationAction = mock(Action.class);
    private long _timeoutValue = 100;
    private long _notificationRepeatPeriod = 5000;

    @Test
    public void testTickWhenNoTransaction() throws Exception
    {
        final long timeNow = System.currentTimeMillis();

        when(_dateSupplier.get()).thenReturn(0L);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);

        assertTickTime("Unexpected ticker value when no transaction is in-progress",
                       Integer.MAX_VALUE,
                       timeNow, _ticker);

        verify(_notificationAction, never()).performAction(anyLong());
    }

    @Test
    public void testTickDuringSingleTransaction() throws Exception
    {
        final long timeNow = System.currentTimeMillis();
        final long transactionTime = timeNow - 90;

        when(_dateSupplier.get()).thenReturn(transactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);

        final int expected = 10;
        assertTickTime("Unexpected ticker value when transaction is in-progress",
                       expected,
                       timeNow, _ticker);

        verify(_notificationAction, never()).performAction(anyLong());
    }

    @Test
    public void testTickDuringSingleTransactionWithSchedulingDelay() throws Exception
    {
        final long timeNow = System.currentTimeMillis();
        final long transactionTime = timeNow - 90;

        when(_dateSupplier.get()).thenReturn(transactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);

        _ticker.notifySchedulingDelay(10);

        final int expected = 20;
        assertTickTime("Unexpected ticker value when transaction is in-progress",
                       expected,
                       timeNow, _ticker);

        verify(_notificationAction, never()).performAction(anyLong());
    }

    @Test
    public void testTicksDuringManyTransactions() throws Exception
    {
        long timeNow = System.currentTimeMillis();
        final long firstTransactionTime = timeNow - 10;

        // First transaction
        when(_dateSupplier.get()).thenReturn(firstTransactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);

        final int expectedTickForFirstTransaction = 90;
        assertTickTime("Unexpected ticker value for first transaction",
                       expectedTickForFirstTransaction,
                       timeNow, _ticker);

        // Second transaction
        timeNow += 100;
        final long secondTransactionTime = timeNow - 5;

        when(_dateSupplier.get()).thenReturn(secondTransactionTime);

        final int expectedTickForSecondTransaction = 95;
        assertTickTime("Unexpected ticker value for second transaction",
                       expectedTickForSecondTransaction,
                       timeNow, _ticker);

        verify(_notificationAction, never()).performAction(anyLong());
    }

    @Test
    public void testTicksDuringManyTransactionsWithSchedulingDelay() throws Exception
    {
        long timeNow = System.currentTimeMillis();
        final long firstTransactionTime = timeNow - 10;

        // First transaction
        when(_dateSupplier.get()).thenReturn(firstTransactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);

        _ticker.notifySchedulingDelay(5);
        final int expectedTickForFirstTransaction = 95;
        assertTickTime("Unexpected ticker value for first transaction",
                       expectedTickForFirstTransaction,
                       timeNow, _ticker);

        // Second transaction. scheduling delay should have been cleared
        timeNow += 100;
        final long secondTransactionTime = timeNow - 5;

        when(_dateSupplier.get()).thenReturn(secondTransactionTime);

        final int expectedTickForSecondTransaction = 95;
        assertTickTime("Unexpected ticker value for second transaction",
                       expectedTickForSecondTransaction,
                       timeNow, _ticker);

        verify(_notificationAction, never()).performAction(anyLong());
    }

    @Test
    public void testSingleTimeoutsDuringSingleTransaction() throws Exception
    {
        long timeNow = System.currentTimeMillis();
        final long transactionTime = timeNow - 110;

        when(_dateSupplier.get()).thenReturn(transactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);


        final long expectedInitialIdle = 110L;
        assertTickerTimeout("Unexpected ticker value when transaction is in-progress",
                            timeNow, _ticker);

        verify(_notificationAction, times(1)).performAction(expectedInitialIdle);
    }

    @Test
    public void testMultipleTimeoutsDuringSingleTransaction_NotificationsRespectPeriod() throws Exception
    {
        InOrder inorder = inOrder(_notificationAction);

        long timeNow = System.currentTimeMillis();
        final long transactionTime = timeNow - 110;

        when(_dateSupplier.get()).thenReturn(transactionTime);

        _ticker = new TransactionTimeoutTicker(_timeoutValue, _notificationRepeatPeriod,
                                               _dateSupplier,
                                               _notificationAction);


        final long expectedInitialIdle = 110L;
        assertTickerTimeout("Unexpected ticker value when transaction is in-progress",
                            timeNow, _ticker);

        inorder.verify(_notificationAction, times(1)).performAction(expectedInitialIdle);

        // Advance the clock by a period of time less than the notification repeat period
        final long timeAdjustment = _notificationRepeatPeriod / 2;
        timeNow += timeAdjustment;

        final long expectedNotificationTick = timeAdjustment;
        assertTickTime("Unexpected ticker value when transaction is in-progress after notification",
                       expectedNotificationTick,
                       timeNow,
                       _ticker);

        inorder.verify(_notificationAction, never()).performAction(anyLong());

        // Advance the clock again past the notification repeat period and that verify that we are notified
        // a second time

        timeNow += timeAdjustment;
        final long expectedSecondIdle = timeNow - transactionTime;
        assertTickerTimeout("Unexpected ticker value when transaction is in-progress and renotification is due",
                            timeNow, _ticker);

        inorder.verify(_notificationAction, times(1)).performAction(expectedSecondIdle);

    }

    private void assertTickerTimeout(final String message,
                                     final long currentTime,
                                     final TransactionTimeoutTicker ticker)
    {
        assertTrue(message, ticker.getTimeToNextTick(currentTime) <= 0);
        assertTrue(message, ticker.tick(currentTime) <= 0);
    }

    private void assertTickTime(final String message,
                                final long expectedValue,
                                final long currentTime,
                                final TransactionTimeoutTicker ticker)
    {
        assertEquals(message, expectedValue, (long) ticker.getTimeToNextTick(currentTime));
        assertEquals(message, expectedValue, (long) ticker.tick(currentTime));
    }
}
