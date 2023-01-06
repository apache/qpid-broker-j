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

import org.apache.qpid.server.store.MessageEnqueueRecord;

/** 
 * Mock implementation of a ServerTransaction Action
 * allowing its state to be observed.
 * 
 */
class MockAction implements ServerTransaction.EnqueueAction, ServerTransaction.Action
{
    private boolean _rollbackFired = false;
    private boolean _postCommitFired = false;

    @Override
    public void postCommit(final MessageEnqueueRecord... records)
    {
        _postCommitFired = true;
    }

    @Override
    public void postCommit()
    {
        _postCommitFired = true;
    }

    @Override
    public void onRollback()
    {
        _rollbackFired = true;
    }

    public boolean isRollbackActionFired()
    {
        return _rollbackFired;
    }

    public boolean isPostCommitActionFired()
    {
        return _postCommitFired;
    }
}
