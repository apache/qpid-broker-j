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
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "qpid/management/addAuthenticationProvider",
        "dojo/_base/event",
        "dijit/registry",
        "dojo/dom-style",
        "dojox/html/entities",
        "dojo/dom",
        "qpid/management/authenticationprovider/PrincipalDatabaseAuthenticationManager",
        "dojo/text!showAuthProvider.html",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              properties,
              updater,
              util,
              UpdatableStore,
              EnhancedGrid,
              addAuthenticationProvider,
              event,
              registry,
              domStyle,
              entities,
              dom,
              PrincipalDatabaseAuthenticationManager,
              template)
    {

        function AuthenticationProvider(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        AuthenticationProvider.prototype.getTitle = function ()
        {
            return "AuthenticationProvider:" + this.name;
        };

        AuthenticationProvider.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {

                    var authProviderUpdater = new AuthProviderUpdater(that);
                    that.authProviderUpdater = authProviderUpdater;

                    var editButtonNode = query(".editAuthenticationProviderButton", contentPane.containerNode)[0];
                    var editButtonWidget = registry.byNode(editButtonNode);
                    editButtonWidget.on("click", function (evt)
                    {
                        event.stop(evt);
                        addAuthenticationProvider.show(that.management,
                            that.modelObj,
                            authProviderUpdater.authProviderData);
                    });
                    authProviderUpdater.editButton = editButtonWidget;

                    var deleteButton = query(".deleteAuthenticationProviderButton", contentPane.containerNode)[0];
                    var deleteWidget = registry.byNode(deleteButton);
                    connect.connect(deleteWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.deleteAuthenticationProvider();
                    });

                    authProviderUpdater.update(function ()
                    {
                        if (that.management.metadata.implementsManagedInterface("AuthenticationProvider",
                                authProviderUpdater.authProviderData.type,
                                "PasswordCredentialManagingAuthenticationProvider"))
                        {
                            authProviderUpdater.managingUsersUI =
                                new PrincipalDatabaseAuthenticationManager(contentPane.containerNode, authProviderUpdater.modelObj, that.controller);
                            authProviderUpdater.managingUsersUI.update(authProviderUpdater.authProviderData);
                        }

                        updater.add(that.authProviderUpdater);
                    });
                });
        };

        AuthenticationProvider.prototype.close = function ()
        {
            updater.remove(this.authProviderUpdater);
            if (this.authProviderUpdater.details)
            {
                updater.remove(this.authProviderUpdater.details.authDatabaseUpdater);
            }
        };

        AuthenticationProvider.prototype.deleteAuthenticationProvider = function ()
        {
            if (confirm("Are you sure you want to delete authentication provider '" + this.name + "'?"))
            {
                var that = this;
                this.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.close();
                        that.contentPane.onClose()
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        };

        function AuthProviderUpdater(authenticationProvider)
        {
            var node = authenticationProvider.contentPane.containerNode;
            this.controller = authenticationProvider.controller;
            this.management = management;
            this.modelObj = authenticationProvider.modelObj;
            this.contentPane = authenticationProvider.contentPane;
            this.name = query(".name", node)[0];
            this.type = query(".type", node)[0];
            this.state = query(".state", node)[0];
            this.tabObject = authenticationProvider;
            this.authenticationProviderDetailsContainer = query(".authenticationProviderDetails", node)[0];
        }

        AuthProviderUpdater.prototype.updateHeader = function ()
        {
            this.tabObject.name = this.authProviderData["name"]
            this.name.innerHTML = entities.encode(String(this.authProviderData["name"]));
            this.type.innerHTML = entities.encode(String(this.authProviderData["type"]));
            this.state.innerHTML = entities.encode(String(this.authProviderData["state"]));
        };

        AuthProviderUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }
            var that = this;
            this.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 1
                })
                .then(function (data)
                {
                    that._update(data);
                    if (callback)
                    {
                        callback();
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Authentication Provider"
                    });
                });
        };

        AuthProviderUpdater.prototype._update = function (data)
        {
            var that = this;
            this.authProviderData = data;
            util.flattenStatistics(data);
            this.updateHeader();

            if (this.details)
            {
                this.details.update(data);
            }
            else
            {
                require(["qpid/management/authenticationprovider/" + encodeURIComponent(data.type.toLowerCase())
                         + "/show"], function (DetailsUI)
                {
                    that.details = new DetailsUI({
                        containerNode: that.authenticationProviderDetailsContainer,
                        parent: that
                    });
                    that.details.update(data);
                });
            }

            if (this.managingUsersUI)
            {
                try
                {
                    this.managingUsersUI.update(data);
                }
                catch (e)
                {
                    if (console)
                    {
                        console.error(e);
                    }
                }
            }
        };

        return AuthenticationProvider;
    });
