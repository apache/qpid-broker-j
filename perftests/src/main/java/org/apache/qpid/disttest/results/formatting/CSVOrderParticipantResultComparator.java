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
package org.apache.qpid.disttest.results.formatting;

import java.util.Comparator;
import java.util.Map;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import org.apache.qpid.disttest.message.ConsumerParticipantResult;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.ProducerParticipantResult;

public class CSVOrderParticipantResultComparator implements Comparator<ParticipantResult>
{
    // TODO yuk
    private static final Map<Class<? extends ParticipantResult>, Integer> TYPE_CODES =
            Map.of(ProducerParticipantResult.class, 0, ConsumerParticipantResult.class, 1, ParticipantResult.class, 2);

    @Override
    public int compare(ParticipantResult left, ParticipantResult right)
    {
        return ComparisonChain.start()
            .compare(getTypeCode(left), getTypeCode(right), Ordering.natural().nullsFirst())
            .compare(left.getParticipantName(), right.getParticipantName(), Ordering.natural().nullsFirst())
            .result();
    }


    private int getTypeCode(ParticipantResult participantResult)
    {
        return TYPE_CODES.get(participantResult.getClass());
    }

}
