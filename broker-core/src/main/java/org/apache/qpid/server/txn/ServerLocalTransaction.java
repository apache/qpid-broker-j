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
package org.apache.qpid.server.txn;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Extension of {@link ServerTransaction} where enqueue/dequeue operations share a single long-lived transaction.
 * <p>
 * The caller is responsible for invoking commit() or rollback() to complete the transaction.
 * If necessary, the transaction can be marked as rollback only.
 */
public interface ServerLocalTransaction extends ServerTransaction
{
    /**
     *  Marks transaction as rollback only
     *
     *  @return true if transaction is marked as read only
     */
    boolean setRollbackOnly();

    /**
     * Indicates whether transaction was marked as rollback only transaction
     *
     * @return true if transaction is rollback only
     */
    boolean isRollbackOnly();

    /**
     * Indicates whether transaction is complete
     *
     * @return true if transaction is complete
     */
    boolean isComplete();

    /**
     * Returns transaction completion future
     *
     * @return transaction completion future
     */
    ListenableFuture<Void> getCompletionFuture();
}
