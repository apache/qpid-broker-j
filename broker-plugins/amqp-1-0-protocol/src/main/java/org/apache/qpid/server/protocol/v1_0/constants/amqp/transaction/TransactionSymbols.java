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

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public interface TransactionSymbols
{
    Symbol AMQP_DISTRIBUTED_TXN = Symbol.valueOf(TransactionSymbolTexts.AMQP_DISTRIBUTED_TXN);
    Symbol AMQP_LOCAL_TXN = Symbol.valueOf(TransactionSymbolTexts.AMQP_LOCAL_TXN);
    Symbol AMQP_MULTI_SESSIONS_PER_TXN = Symbol.valueOf(TransactionSymbolTexts.AMQP_MULTI_SESSIONS_PER_TXN);
    Symbol AMQP_MULTI_TXN_PER_SESSION = Symbol.valueOf(TransactionSymbolTexts.AMQP_MULTI_TXN_PER_SESSION);
    Symbol AMQP_PROMOTABLE_TXN = Symbol.valueOf(TransactionSymbolTexts.AMQP_PROMOTABLE_TXN);
    Symbol AMQP_TXN_COORDINATOR = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_COORDINATOR);
    Symbol AMQP_TXN_DECLARE = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_DECLARE);
    Symbol AMQP_TXN_DECLARED = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_DECLARED);
    Symbol AMQP_TXN_DISCHARGE = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_DISCHARGE);
    Symbol AMQP_TXN_ROLLBACK = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_ROLLBACK);
    Symbol AMQP_TXN_STATE = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_STATE);
    Symbol AMQP_TXN_TIMEOUT = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_TIMEOUT);
    Symbol AMQP_TXN_UNKNOWN_ID = Symbol.valueOf(TransactionSymbolTexts.AMQP_TXN_UNKNOWN_ID);
    Symbol TXN_ID = Symbol.valueOf(TransactionSymbolTexts.TXN_ID);
}
