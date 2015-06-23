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
define(["dojo/dom",
        "dijit/registry",
        "dijit/layout/ContentPane",
        "dijit/form/CheckBox",
        "dojox/html/entities",
        "qpid/management/Broker",
        "qpid/management/VirtualHost",
        "qpid/management/Exchange",
        "qpid/management/Queue",
        "qpid/management/Connection",
        "qpid/management/AuthenticationProvider",
        "qpid/management/GroupProvider",
        "qpid/management/group/Group",
        "qpid/management/KeyStore",
        "qpid/management/TrustStore",
        "qpid/management/AccessControlProvider",
        "qpid/management/Port",
        "qpid/management/Plugin",
        "qpid/management/logs/LogViewer",
        "qpid/management/PreferencesProvider",
        "qpid/management/VirtualHostNode",
        "qpid/management/Logger",
        "dojo/ready",
        "dojo/domReady!"],
       function (dom, registry, ContentPane, CheckBox, entities, Broker, VirtualHost, Exchange, Queue, Connection, AuthProvider,
                 GroupProvider, Group, KeyStore, TrustStore, AccessControlProvider, Port, Plugin, LogViewer, PreferencesProvider, VirtualHostNode, Logger, ready) {
           var controller = {};

           var constructors = { broker: Broker, virtualhost: VirtualHost, exchange: Exchange,
                                queue: Queue, connection: Connection,
                                authenticationprovider: AuthProvider, groupprovider: GroupProvider,
                                group: Group, keystore: KeyStore, truststore: TrustStore,
                                accesscontrolprovider: AccessControlProvider, port: Port,
                                plugin: Plugin, logViewer: LogViewer, preferencesprovider: PreferencesProvider,
                                virtualhostnode: VirtualHostNode, brokerlogger: Logger, virtualhostlogger: Logger};

           var tabDiv = dom.byId("managedViews");

           ready(function() {
               controller.tabContainer = registry.byId("managedViews");
           });


           controller.viewedObjects = {};

           controller.show = function(objType, name, parent, objectId) {

               function generateName(obj)
               {
                    if(obj) {
                        var name = obj.type + (obj.type == "broker" ? "" : ":" + obj.name);
                        if (obj.parent)
                        {
                            name = generateName(obj.parent) + "/" + name;
                        }
                        return name;
                    }
                    return "";
               }

               var that = this;
               var objId = (parent ? generateName(parent) + "/" : "") + objType + ":" + name;

               var obj = this.viewedObjects[ objId ];
               if(obj) {
                   this.tabContainer.selectChild(obj.contentPane);
               } else {
                   var Constructor = constructors[ objType ];
                   if(Constructor) {
                       obj = new Constructor(name, parent, this);
                       obj.tabData = {
                           objectId: objectId,
                           objectType: objType
                       };
                       this.viewedObjects[ objId ] = obj;

                       var contentPane = new ContentPane({ region: "center" ,
                                                           title: entities.encode(obj.getTitle()),
                                                           closable: true,
                                                           onClose: function() {
                                                               obj.close();
                                                               delete that.viewedObjects[ objId ];
                                                               return true;
                                                           }
                       });
                       this.tabContainer.addChild( contentPane );
                       var userPreferences = this.management.userPreferences;
                       if (objType != "broker")
                       {
                         var preferencesCheckBox = new dijit.form.CheckBox({
                           checked: userPreferences.isTabStored(obj.tabData),
                           title: "If checked the tab is saved in user preferences and restored on next login"
                         });
                         var tabs = this.tabContainer.tablist.getChildren();
                         preferencesCheckBox.placeAt(tabs[tabs.length-1].titleNode, "first");
                         preferencesCheckBox.on("change", function(value){
                           if (value)
                           {
                             userPreferences.appendTab(obj.tabData);
                           }
                           else
                           {
                             userPreferences.removeTab(obj.tabData);
                           }
                         });
                       }
                       obj.open(contentPane);
                       contentPane.startup();
                       if(obj.startup) {
                           obj.startup();
                       }
                       this.tabContainer.selectChild( contentPane );
                   }

               }

           };

            controller.init=function(management)
            {
                controller.management = management;
            }

           return controller;
       });
