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
define(["dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "qpid/common/updater",
        "qpid/common/util",
        "dijit/registry",
        "dojo/_base/event",
        "dojox/html/entities",
        "dojo/text!showConnectionLimitProvider.html",
        "dijit/TitlePane",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              updater,
              util,
              registry,
              event,
              entities,
              template)
    {
        function ConnectionLimitProvider(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = kwArgs.controller.management;
            this.name = kwArgs.tabData.modelObject.name;
        }

        ConnectionLimitProvider.prototype.getTitle = function ()
        {
            return "ConnectionLimitProvider: " + this.name;
        };

        ConnectionLimitProvider.prototype.open = function (contentPane)
        {
            const that = this;
            this.contentPane = contentPane;

            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function ()
                {
                    const deleteButton = query(".deleteConnectionLimitProviderButton", contentPane.containerNode)[0];
                    const deleteWidget = registry.byNode(deleteButton);
                    connect.connect(deleteWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.deleteConnectionLimitProvider();
                    });

                    that._ConnectionLimitProviderUpdater = new ConnectionLimitProviderUpdater(that);
                    that._ConnectionLimitProviderUpdater.update(function ()
                    {
                        updater.add(that._ConnectionLimitProviderUpdater);
                    });
                });
        };

        ConnectionLimitProvider.prototype.close = function ()
        {
            updater.remove(this._ConnectionLimitProviderUpdater);
        };

        ConnectionLimitProvider.prototype.deleteConnectionLimitProvider = function ()
        {
            if (confirm("Are you sure you want to delete user connection limit provider '" + this.name + "'?"))
            {
                const that = this;
                this.controller.management.remove(this.modelObj)
                    .then(function ()
                    {
                        that.close();
                        that.contentPane.onClose()
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        };

        function ConnectionLimitProviderUpdater(tab)
        {
            this.contentPane = tab.contentPane;
            this.controller = tab.controller;
            this.management = this.controller.management;
            this.modelObj = tab.modelObj;

            const node = tab.contentPane.containerNode;
            this._name = query(".name", node)[0];
            this._type = query(".type", node)[0];
            this._state = query(".state", node)[0];
            this._providerDetails = query(".providerDetails", node)[0];
        }

        ConnectionLimitProviderUpdater.prototype.updateHeader = function ()
        {
            this._name.innerHTML = entities.encode(String(this._connectionLimitProviderData["name"]));
            this._type.innerHTML = entities.encode(String(this._connectionLimitProviderData["type"]));
            this._state.innerHTML = entities.encode(String(this._connectionLimitProviderData["state"]));
        };

        ConnectionLimitProviderUpdater.prototype.update = function (callback) {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            const that = this;
            this.management
                .load(this.modelObj)
                .then(function (data) {
                        that._update(data, callback);
                    },
                    function (error) {
                        util.tabErrorHandler(error, {
                            updater: that,
                            contentPane: that.contentPane,
                            tabContainer: that.controller.tabContainer,
                            name: that.modelObj.name,
                            category: "User Connection Limit Provider"
                        });
                    });
        };

        ConnectionLimitProviderUpdater.prototype._update = function (data, callback) {
            this._connectionLimitProviderData = data;
            this.updateHeader();
            if (this._specificProvider)
            {
                this._specificProvider.update(data);
            }
            else
            {
                const that = this;
                require(["qpid/management/connectionlimitprovider/" + data.type.toLowerCase() + "/show"],
                    function (SpecificProvider) {
                        that._specificProvider = new SpecificProvider(that._providerDetails, that.modelObj, that.controller);
                        that._specificProvider.update(data);
                        if (callback)
                        {
                            callback();
                        }
                    });
            }
        };

        return ConnectionLimitProvider;
    });
