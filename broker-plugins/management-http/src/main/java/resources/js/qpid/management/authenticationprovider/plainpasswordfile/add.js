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
define(["dojo/query", "dijit/registry", "qpid/common/util", "dojo/dom-class"], function (query, registry, util, domClass)
{
    return {
        show: function (data)
        {
            util.parseHtmlIntoDiv(data.containerNode, "authenticationprovider/filebased/add.html", function ()
            {
                var path = registry.byNode(query(".path", data.containerNode)[0]);
                if (data.data && data.data.id)
                {
                    path.set("value", data.data.path);
                    path.set("readOnly", true);
                    domClass.add(path.domNode, "readOnly")
                }
                else
                {
                    path.set("readOnly", false);
                    domClass.remove(path.domNode, "readOnly")
                }
            });
        }
    };
});

