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
    private final double _bias;
    private StepPolicy _stepPolicy;

    public HillClimber(double initialValue, double initialDelta)
    {
        this(initialValue, initialDelta, 0.5);
    }

    public HillClimber(double initialValue, double initialDelta, double bias)
    {
        if (bias <= 0 || 1 <= bias)
        {
            throw new IllegalArgumentException("HillClimber bias must be in the open interval (0, 1) got: " + bias);
        }
        _bias = bias;
        _stepPolicy = new UnknownDirectionStepPolicy(initialValue, initialDelta);
    }

    public double nextHigher()
    {
        return _stepPolicy.nextHigher();
    }

    public double nextLower()
    {
        return _stepPolicy.nextLower();
    }

    public double getCurrentDelta()
    {
        return _stepPolicy.getCurrentDelta();
    }

    public double getBias()
    {
        return _bias;
    }

    private interface StepPolicy
    {
        double getCurrentDelta();
        double nextHigher();
        double nextLower();
    }

    private abstract class AbstractStepPolicy implements StepPolicy
    {
        protected double _value;
        protected double _stepSize;

        @Override
        public double getCurrentDelta()
        {
            return _stepSize;
        }
    }

    private class UnknownDirectionStepPolicy extends AbstractStepPolicy
    {
        UnknownDirectionStepPolicy(final double initialValue, final double initialStepSize)
        {
            _value = initialValue;
            _stepSize = initialStepSize;
        }

        @Override
        public double nextHigher()
        {
            _stepPolicy = new ExponentialUpStepPolicy(_value, _stepSize);
            return _stepPolicy.nextHigher();
        }

        @Override
        public double nextLower()
        {
            _stepPolicy = new ExponentialDownStepPolicy(_value, _stepSize);
            return _stepPolicy.nextLower();
        }
    }

    private class ExponentialUpStepPolicy extends AbstractStepPolicy
    {
        private double _lastValue;

        public ExponentialUpStepPolicy(final double initialValue, final double initialStepSize)
        {
            _value = _lastValue = initialValue;
            _stepSize = initialStepSize;
        }

        @Override
        public double nextHigher()
        {
            _lastValue = _value;
            _value += _stepSize;
            _stepSize *= 2;
            return _value;
        }

        @Override
        public double nextLower()
        {
            _stepPolicy = new BisectionStepPolicy(_lastValue, _value, _value);
            return _stepPolicy.nextLower();
        }
    }

    private class ExponentialDownStepPolicy extends AbstractStepPolicy
    {
        private double _lastValue;

        public ExponentialDownStepPolicy(final double initialValue, final double initialStepSize)
        {
            this._value = _lastValue = initialValue;
            this._stepSize = initialStepSize;
        }

        @Override
        public double nextHigher()
        {
            _stepPolicy = new BisectionStepPolicy(_value, _lastValue, _value);
            return _stepPolicy.nextHigher();
        }

        @Override
        public double nextLower()
        {
            _lastValue = _value;
            _value -= _stepSize;
            _stepSize *= 2;
            return _value;
        }
    }

    private class BisectionStepPolicy extends AbstractStepPolicy
    {
        private double _lowerBound;
        private double _upperBound;

        public BisectionStepPolicy(double lowerBound, double upperBound, double currentValue)
        {
            _lowerBound = lowerBound;
            _upperBound = upperBound;
            _value = currentValue;
        }

        @Override
        public double getCurrentDelta()
        {
            return _upperBound - _lowerBound;
        }

        @Override
        public double nextHigher()
        {
            _lowerBound = _value;
            return step();
        }

        @Override
        public double nextLower()
        {
            _upperBound = _value;
            return step();
        }

        private double step()
        {
            double delta = getCurrentDelta();
            _value = _lowerBound + getBias() * delta;
            return _value;
        }

    }
}
