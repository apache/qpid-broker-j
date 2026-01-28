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

package org.apache.qpid.server.protocol.v1_0.constants.amqp.transaction;

public interface TransactionSymbolTexts
{
    String AMQP_DISTRIBUTED_TXN = "amqp:distributed-transactions";
    String AMQP_LOCAL_TXN = "amqp:local-transactions";
    String AMQP_MULTI_SESSIONS_PER_TXN = "amqp:multi-ssns-per-txn";
    String AMQP_MULTI_TXN_PER_SESSION = "amqp:multi-txns-per-ssn";
    String AMQP_PROMOTABLE_TXN = "amqp:promotable-transactions";
    String AMQP_TXN_COORDINATOR = "amqp:coordinator:list";
    String AMQP_TXN_DECLARE = "amqp:declare:list";
    String AMQP_TXN_DECLARED = "amqp:declared:list";
    String AMQP_TXN_DISCHARGE = "amqp:discharge:list";
    String AMQP_TXN_ROLLBACK = "amqp:transaction:rollback";
    String AMQP_TXN_STATE = "amqp:transactional-state:list";
    String AMQP_TXN_TIMEOUT = "amqp:transaction:timeout";
    String AMQP_TXN_UNKNOWN_ID = "amqp:transaction:unknown-id";
    String TXN_ID = "txn-id";
}
