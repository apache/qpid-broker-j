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

package org.apache.qpid.tests.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(QpidTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BrokerAdminUsingTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAdminUsingTestBase.class);

    private BrokerAdmin _brokerAdmin;

    /**
     * JUnit TestInfo
     */
    protected TestInfo _testInfo;

    @BeforeEach
    public void init(final TestInfo testInfo)
    {
        _testInfo = testInfo;
    }

    public BrokerAdminUsingTestBase setBrokerAdmin(final BrokerAdmin brokerAdmin)
    {
        _brokerAdmin = brokerAdmin;
        return this;
    }

    public BrokerAdmin getBrokerAdmin()
    {
        return _brokerAdmin;
    }

    protected String getTestClass()
    {
        return _testInfo.getTestClass().orElseThrow(() -> new RuntimeException("Failed to resolve test class")).getSimpleName();
    }

    protected String getTestName()
    {
        return _testInfo.getTestMethod().orElseThrow(() -> new RuntimeException("Failed to resolve test method")).getName();
    }

    protected String getFullTestName()
    {
        return getTestClass() + "_" + getTestName();
    }
}
