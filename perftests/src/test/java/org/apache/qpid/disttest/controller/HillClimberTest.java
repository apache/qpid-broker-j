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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class HillClimberTest extends UnitTestBase
{
    final static double EPSILON = 1e-6;

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testExhaustiveCoverage()
    {
        assertExhaustiveCoverageForBias(0.5);
        assertExhaustiveCoverageForBias(0.25);
        assertExhaustiveCoverageForBias(0.66);
    }

    @Test
    public void testRejectInvalidBias()
    {
        try
        {
            new HillClimber(10, 16, -0.1);
            fail("HillClimber bias should not accept negative values");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            new HillClimber(10, 16, 0);
            fail("HillClimber bias should not accept values smaller or equal to 0");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            new HillClimber(10, 16, 1.1);
            fail("HillClimber bias should not accept values larger or equal to 1");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            new HillClimber(10, 16, 1);
            fail("HillClimber bias should not accept values larger or equal to 1");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    private void assertExhaustiveCoverageForBias(final double bias)
    {
        int numberOfBits = 5;
        int maxValue = 1<<numberOfBits;
        boolean[] valuesSeen = new boolean[maxValue];
        double maxBias = Math.max(bias, 1-bias);
        int maxHillClimberSteps = (int) Math.floor(-numberOfBits / log(maxBias, 2));
        for (int bitPattern = 0; bitPattern < 1<<maxHillClimberSteps; ++bitPattern)
        {
            HillClimber hillClimber = new HillClimber(0, maxValue, bias);
            double finalValue = -1;
            // step twice to avoid initial exponential phase and position ourselves in the middle of the range
            hillClimber.nextHigher();
            hillClimber.nextLower();
            for (int step = 0; step < maxHillClimberSteps; ++step)
            {
                boolean stepUp = getBit(bitPattern, maxHillClimberSteps - step - 1);
                if (stepUp)
                {
                    finalValue = hillClimber.nextHigher();
                }
                else
                {
                    finalValue = hillClimber.nextLower();
                }
                if (hillClimber.getCurrentDelta() < 1)
                {
                    break;
                }
            }
            valuesSeen[(int)finalValue] = true;
        }
        for (int i = 0; i < maxValue; ++i)
        {
            assertTrue(valuesSeen[i], "HillClimber with bias=" + bias + " missed value " + i);
        }
    }

    private static boolean getBit(long x, int bit)
    {
        return (x & (1<<bit)) != 0;
    }

    private static double log(double x, double base)
    {
        return Math.log(x) / Math.log(base);
    }
}
