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
define(["dojo/dom",
        "qpid/common/util",
        "qpid/common/ResourceWidget"],
    function (dom, util)
    {
        return {
            show: function (data)
            {
                util.parseHtmlIntoDiv(data.containerNode,
                    "connectionlimitprovider/connectionlimitfile/add.html",
                    function ()
                    {
                        if (!window.FileReader)
                        {
                            const odBrowserWarning = dom.byId("addConnectionLimitProvider.oldBrowserWarning");
                            odBrowserWarning.innerHTML = "File upload requires a more recent browser with HTML5 support";
                            odBrowserWarning.className = odBrowserWarning.className.replace("hidden", "");
                        }

                        util.applyToWidgets(data.containerNode,
                            data.category,
                            data.type,
                            data.initialData,
                            data.metadata,
                            data.effectiveData);
                    });
            }
        };
    });
