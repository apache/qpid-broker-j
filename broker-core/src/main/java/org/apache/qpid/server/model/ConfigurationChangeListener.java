/*
 *
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
package org.apache.qpid.server.model;

public interface ConfigurationChangeListener
{
    /**
     * Inform the listener that the passed object has changed state
     *
     * @param object the object whose state has changed
     * @param oldState the state prior to the change
     * @param newState the state after the change
     */
    void stateChanged(ConfiguredObject<?> object, State oldState, State newState);

    void childAdded(ConfiguredObject<?> object, ConfiguredObject<?> child);

    void childRemoved(ConfiguredObject<?> object, ConfiguredObject<?> child);

    void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue);

    /**
     * Inform the listener that several attributes of an object are about to change.
     *
     * The listener may choose to defer any action in attributeSet until bulkChangeEnd is called.
     * There should not be multiple calls to bulkChangeStart without matching bulkChangeEnd calls in between.
     * There should be no calls to attributeSet for objects other than the one passed as an argument until bulkChangeEnd is called.
     * There should be no call to childRemove between bulkChangeStart/-End calls.
     * @param object the object whose state is about to change
     * @see #bulkChangeEnd
     */
    void bulkChangeStart(ConfiguredObject<?> object);

    /**
     * Inform the listener that the changes announced by bulkChangeStart are complete.
     *
     * The listener who has chosen to defer any action in attributeSet after bulkChangeStart was called should now act on those changes.
     * A call to bulkChangeEnd without a prior matching call to bulkChangeStart should have no effect.
     * @param object the object whose state has changed
     * @see #bulkChangeStart
     */
    void bulkChangeEnd(ConfiguredObject<?> object);
}
