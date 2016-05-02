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

define(["qpid/common/util", "dojox/html/entities", "dojo/domReady!"], function (util, entities)
{

    function NonJavaKeyStore(data)
    {
        var that = this;
        this.fields = [];
        this.management = data.parent.management;
        this.dateTimeFormatter = function (value)
        {
            return value ? that.management.userPreferences.formatDateTime(value, {
                addOffset: true,
                appendTimeZone: true
            }) : "";
        };

        var attributes = this.management.metadata.getMetaData("KeyStore", "NonJavaKeyStore").attributes;
        for (var name in attributes)
        {
            this.fields.push(name);
        }
        util.buildUI(data.containerNode, data.parent, "store/nonjavakeystore/show.html", this.fields, this);
    }

    NonJavaKeyStore.prototype.update = function (data)
    {
        util.updateUI(data, this.fields, this, {datetime: this.dateTimeFormatter});
    }

    return NonJavaKeyStore;
});
