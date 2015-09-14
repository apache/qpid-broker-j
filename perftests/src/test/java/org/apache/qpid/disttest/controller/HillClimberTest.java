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
 */
package org.apache.qpid.disttest.controller;

import org.apache.qpid.test.utils.QpidTestCase;

public class HillClimberTest extends QpidTestCase
{
    final static double EPSILON = 1e-6;

    public void testHomingIn()
    {
        HillClimber hillClimber = new HillClimber(0, 16);
        assertEquals(16, hillClimber.nextHigher(), EPSILON);
        assertEquals(8, hillClimber.nextLower(), EPSILON);
        assertEquals(4, hillClimber.nextLower(), EPSILON);
        assertEquals(2, hillClimber.nextLower(), EPSILON);
        assertEquals(1, hillClimber.nextLower(), EPSILON);
        assertEquals(0.5, hillClimber.nextLower(), EPSILON);
    }

    public void testHomingIn2()
    {
        HillClimber hillClimber = new HillClimber(0, 16);
        assertEquals(16, hillClimber.nextHigher(), EPSILON);
        assertEquals(8, hillClimber.nextLower(), EPSILON);
        assertEquals(12, hillClimber.nextHigher(), EPSILON);
        assertEquals(14, hillClimber.nextHigher(), EPSILON);
        assertEquals(13, hillClimber.nextLower(), EPSILON);
        assertEquals(12.5, hillClimber.nextLower(), EPSILON);
    }

    public void testHomingInNegative()
    {
        HillClimber hillClimber = new HillClimber(0, 16);
        assertEquals(-16, hillClimber.nextLower(), EPSILON);
        assertEquals(-8, hillClimber.nextHigher(), EPSILON);
        assertEquals(-4, hillClimber.nextHigher(), EPSILON);
        assertEquals(-2, hillClimber.nextHigher(), EPSILON);
        assertEquals(-3, hillClimber.nextLower(), EPSILON);
        assertEquals(-3.5, hillClimber.nextLower(), EPSILON);
    }

    public void testExpSearch()
    {
        HillClimber hillClimber = new HillClimber(10, 16);
        assertEquals(-16 + 10, hillClimber.nextLower(), EPSILON);
        assertEquals(-48 + 10, hillClimber.nextLower(), EPSILON);
        assertEquals(-112 + 10, hillClimber.nextLower(), EPSILON);
        assertEquals(-80 + 10, hillClimber.nextHigher(), EPSILON);
        assertEquals(-96 + 10, hillClimber.nextLower(), EPSILON);
        assertEquals(-104 + 10, hillClimber.nextLower(), EPSILON);
    }
}
