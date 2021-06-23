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
package org.apache.qpid.server.security.limit;

import java.util.Optional;

@FunctionalInterface
public interface ConnectionSlot extends Runnable
{
    void free();

    @Override
    default void run()
    {
        free();
    }

    default ConnectionSlot chainTo(final ConnectionSlot secondarySlot)
    {
        if (secondarySlot == null || secondarySlot instanceof FreeSlot)
        {
            return this;
        }
        final ConnectionSlot primarySlot = this;
        return () ->
        {
            try
            {
                secondarySlot.free();
            }
            finally
            {
                primarySlot.free();
            }
        };
    }

    final class FreeSlot implements ConnectionSlot
    {
        public static final FreeSlot INSTANCE = new FreeSlot();

        private FreeSlot()
        {
            super();
        }

        @Override
        public void free()
        {
            // Do nothing
        }

        @Override
        public ConnectionSlot chainTo(ConnectionSlot secondarySlot)
        {
            return Optional.ofNullable(secondarySlot).orElse(this);
        }
    }
}
