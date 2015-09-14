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

public class HillClimber
{
    private double _value;
    private double _delta;
    private boolean _expSearch = true;
    private int _direction = 0;

    public HillClimber(double initialValue, double initialDelta)
    {
        _value = initialValue;
        _delta = initialDelta / 2.0;
    }

    public double nextHigher()
    {
        if (_direction == 0)
        {
            _direction = 1;
        }
        else if (_direction == -1)
        {
            _expSearch = false;
            _direction = 1;
        }

        return step();
    }

    public double nextLower()
    {
        if (_direction == 0)
        {
            _direction = -1;
        }
        else if (_direction == 1)
        {
            _expSearch = false;
            _direction = -1;
        }

        return step();
    }

    private double step()
    {
        if (_expSearch)
        {
            _delta *= 2;
            _value += _direction * _delta;
        }
        else
        {
            _delta /= 2.0;
            _value += _direction * _delta;
        }
        return _value;
    }

    public double getCurrentDelta()
    {
        return _delta;
    }
}
