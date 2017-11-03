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
define(["dojo/dom", "qpid/common/util", "qpid/common/ResourceWidget"],
    function (dom, util)
    {
        return {
            show: function (data)
            {
                var that = this;
                util.parseHtmlIntoDiv(data.containerNode,
                    "accesscontrolprovider/aclfile/add.html",
                    function ()
                    {
                        that._postParse(data);
                    });
            },
            _postParse: function (data)
            {
                var aclFileOldBrowserWarning = dom.byId("addAccessControlProvider.oldBrowserWarning");

                var reader = window.FileReader ? new FileReader() : undefined;
                if (!reader)
                {
                    aclFileOldBrowserWarning.innerHTML =
                        "File upload requires a more recent browser with HTML5 support";
                    aclFileOldBrowserWarning.className =
                        aclFileOldBrowserWarning.className.replace("hidden", "");
                }

                util.applyToWidgets(data.containerNode,
                    "AccessControlProvider",
                    "AclFile",
                    data.initialData,
                    data.metadata,
                    data.effectiveData);

            }
        };
    });
