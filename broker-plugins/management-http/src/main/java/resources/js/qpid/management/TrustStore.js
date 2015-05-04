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
        "dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/management/addStore",
        "dojo/text!showStore.html",
        "dojo/domReady!"],
       function (dom, parser, query, connect, registry, entities, properties, updater, util, formatter, addStore, template) {

           function TrustStore(name, parent, controller) {
               this.keyStoreName = name;
               this.controller = controller;
               this.modelObj = { type: "truststore", name: name, parent: parent};
               this.management = controller.management;
           }

           TrustStore.prototype.getTitle = function() {
               return "TrustStore: " + this.keyStoreName;
           };

           TrustStore.prototype.open = function(contentPane) {
               var that = this;
               this.contentPane = contentPane;

                contentPane.containerNode.innerHTML = template;
                parser.parse(contentPane.containerNode).then(function(instances)
                {

                            that.keyStoreUpdater = new KeyStoreUpdater(contentPane.containerNode, that.modelObj, that.controller);
                            that.keyStoreUpdater.update(function(){updater.add( that.keyStoreUpdater );});

                            var deleteTrustStoreButton = query(".deleteStoreButton", contentPane.containerNode)[0];
                            var node = registry.byNode(deleteTrustStoreButton);
                            connect.connect(node, "onClick",
                                function(evt){
                                    that.deleteKeyStore();
                                });

                            var editTrustStoreButton = query(".editStoreButton", contentPane.containerNode)[0];
                            var node = registry.byNode(editTrustStoreButton);
                            connect.connect(node, "onClick",
                                function(evt){
                                    that.management.load(that.modelObj, { actuals: true })
                                    .then(function(data)
                                    {
                                      addStore.setupTypeStore(that.management, "TrustStore", that.modelObj);
                                      addStore.show(data[0], that.url);
                                    }, util.xhrErrorHandler);
                                });
                });
           };

           TrustStore.prototype.close = function() {
               updater.remove( this.keyStoreUpdater );
           };

           function KeyStoreUpdater(containerNode, keyStoreObj, controller, url)
           {
               var that = this;
               this.keyStoreDetailsContainer = query(".typeFieldsContainer", containerNode)[0];
               this.management = controller.management;
               this.modelObj = keyStoreObj;

               function findNode(name) {
                   return query("." + name , containerNode)[0];
               }

               function storeNodes(names)
               {
                  for(var i = 0; i < names.length; i++) {
                      that[names[i]] = findNode(names[i]);
                  }
               }

               storeNodes(["name",
                           "type",
                           "state"
                           ]);

               this.query = url;

           }

           KeyStoreUpdater.prototype.updateHeader = function()
           {
              this.name.innerHTML = entities.encode(String(this.trustStoreData[ "name" ]));
              this.type.innerHTML = entities.encode(String(this.trustStoreData[ "type" ]));
              this.state.innerHTML = entities.encode(String(this.trustStoreData[ "state" ]));
           };

           KeyStoreUpdater.prototype.update = function(callback)
           {
              var that = this;
              this.management.load(this.modelObj).then(function(data)
               {
                  that.trustStoreData = data[0];
                  that.updateHeader();

                  if (callback)
                  {
                    callback();
                  }

                  if (that.details)
                  {
                    that.details.update(that.trustStoreData);
                  }
                  else
                  {
                    require(["qpid/management/store/" + encodeURIComponent(that.trustStoreData.type.toLowerCase()) + "/show"],
                      function(DetailsUI)
                      {
                        that.details = new DetailsUI({containerNode:that.keyStoreDetailsContainer, parent: that});
                        that.details.update(that.trustStoreData);
                      }
                    );
                  }
               });
           };

           TrustStore.prototype.deleteKeyStore = function() {
               if(confirm("Are you sure you want to delete trust store '" +this.keyStoreName+"'?")) {
                   var that = this;
                   this.management.remove(this.modelObj).then(
                       function(data) {
                           that.contentPane.onClose()
                           that.controller.tabContainer.removeChild(that.contentPane);
                           that.contentPane.destroyRecursive();
                           that.close();
                       },
                       util.xhrErrorHandler);
               }
           }

           return TrustStore;
       });
