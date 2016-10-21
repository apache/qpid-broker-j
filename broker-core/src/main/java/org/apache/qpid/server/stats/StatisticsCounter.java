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
package org.apache.qpid.server.stats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class collects statistics and counts the total, rate per second and
 * peak rate per second values for the events that are registered with it. 
 */
public class StatisticsCounter
{
    private static final Logger _log = LoggerFactory.getLogger(StatisticsCounter.class);
    
    public static final long DEFAULT_SAMPLE_PERIOD = Long.getLong("qpid.statistics.samplePeriod", 2000L); // 2s

    private static final String COUNTER = "counter";
    private static final AtomicLong _counterIds = new AtomicLong(0L);

    private final long _period;
    private final String _name;

    public StatisticsCounter()
    {
        this(COUNTER);
    }
    
    public StatisticsCounter(String name)
    {
        this(name, DEFAULT_SAMPLE_PERIOD);
    }

    public StatisticsCounter(String name, long period)
    {
        _period = period;
        _name = name + "-" + + _counterIds.incrementAndGet();

        _currentSample.set(new Sample(period));
    }

    private static final class Sample
    {
        private final long _sampleId;
        private final AtomicLong _count = new AtomicLong();
        private final AtomicLong _total;
        private final long _peak;
        private final long _lastRate;
        private final long _start;
        private final long _period;

        private Sample(final long period)
        {
            _period = period;
            _total = new AtomicLong();
            _peak = 0L;
            _lastRate = 0L;
            _start = System.currentTimeMillis();
            _sampleId = _start / period;

        }

        private Sample(final long timestamp, Sample priorSample)
        {
            _period = priorSample._period;
            _sampleId = timestamp / _period;
            _total = priorSample._total;
            _peak = priorSample.getRate() > priorSample.getPeak() ? priorSample.getRate() : priorSample.getPeak();
            _lastRate = priorSample.getRate();
            _start = priorSample._start;
        }

        public long getTotal()
        {
            return _total.get();
        }

        public long getRate()
        {
            return _count.get();
        }

        public long getPeak()
        {
            return _peak;
        }

        public long getLastRate()
        {
            return _lastRate;
        }

        public long getStart()
        {
            return _start;
        }

        public boolean add(final long value, final long timestamp)
        {
            if(timestamp >= _start)
            {
                long eventSampleId = timestamp / _period;
                if(eventSampleId > _sampleId)
                {
                    return false;
                }
                _total.addAndGet(value);
                if(eventSampleId == _sampleId)
                {
                    _count.addAndGet(value);
                }
                return true;
            }
            else
            {
                // ignore - event occurred before reset;
                return true;
            }
        }
    }

    private AtomicReference<Sample> _currentSample = new AtomicReference<>();


    public void registerEvent(long value)
    {
        registerEvent(value, System.currentTimeMillis());
    }

    public void registerEvent(long value, long timestamp)
    {
        Sample currentSample;

        while(!(currentSample = getSample()).add(value, timestamp))
        {
            Sample nextSample = new Sample(timestamp, currentSample);
            _currentSample.compareAndSet(currentSample, nextSample);
        }
    }
    
    /**
     * Update the current rate and peak - may reset rate to zero if a new
     * sample period has started.
     */
    private void update()
    {
        registerEvent(0L, System.currentTimeMillis());
    }

    /**
     * Reset 
     */
    public void reset()
    {
        _log.info("Resetting statistics for counter: " + _name);

        _currentSample.set(new Sample(_period));
    }

    public double getPeak()
    {
        update();
        return (double) getSample().getPeak() / ((double) _period / 1000.0d);
    }

    private Sample getSample()
    {
        return _currentSample.get();
    }

    public double getRate()
    {
        update();
        return (double) getSample().getLastRate() / ((double) _period / 1000.0d);
    }

    public long getTotal()
    {
        return getSample().getTotal();
    }

    public long getStart()
    {
        return getSample().getStart();
    }

    public String getName()
    {
        return _name;
    }
    
    public long getPeriod()
    {
        return _period;
    }
}
