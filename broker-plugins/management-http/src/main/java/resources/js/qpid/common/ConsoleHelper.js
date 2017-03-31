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
define(["dojo/domReady!"], function () {

    var preferencesDialog = null;
    var helpURL = null;

    var documentationUrl = null;

    var openWindow = function (url, title)
    {
        var newWindow = window.open(
            url,
            title,
            'height=600,width=600,scrollbars=1,location=1,resizable=1,status=0,toolbar=0,titlebar=1,menubar=0',
            true);
        newWindow.focus();
    };

    return {
        showPreferencesDialog: function () {
          if (preferencesDialog == null)
          {
             require(["qpid/management/Preferences", "dojo/ready"], function(PreferencesDialog, ready){
                ready(function(){
                  preferencesDialog = new PreferencesDialog(this.management);
                  preferencesDialog.showDialog();
                });
             });
          }
          else
          {
              preferencesDialog.showDialog();
          }
        },
        showDocumentation: function ()
        {
            if (documentationUrl)
            {
                openWindow(documentationUrl, "Qpid Documentation");
            }
            else
            {
                this.management.load({type: "broker"}, {depth: 0, excludeInheritedContext: true})
                    .then(function (data)
                    {
                        var broker = data[0];
                        if (broker.documentationUrl)
                        {
                            documentationUrl = broker.documentationUrl;
                        }
                        else
                        {
                            documentationUrl = "http://qpid.apache.org/components/java-broker/";
                        }
                        openWindow(documentationUrl, "Qpid Documentation")
                    });
            }
        },
        showAPI: function()
        {
            openWindow(getContextPath() + "apidocs", 'Qpid REST API');
        }

    };

});
