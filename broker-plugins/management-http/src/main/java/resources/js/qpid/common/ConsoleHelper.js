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
        getHelpUrl: function(callback)
        {
            this.management.load({type: "broker"}, {depth: 0}).then(
             function(data) {
              var broker = data[0];
              if ("context" in broker && "qpid.helpURL" in broker["context"] )
              {
                helpURL = broker["context"]["qpid.helpURL"];
              }
              else
              {
                helpURL = "http://qpid.apache.org/";
              }
              if (callback)
              {
                callback(helpURL);
              }
             });
        },
        showHelp: function()
        {
          var openWindow = function(url)
          {
              var newWindow = window.open(url,'QpidHelp','height=600,width=600,scrollbars=1,location=1,resizable=1,status=0,toolbar=0,titlebar=1,menubar=0', true);
              newWindow.focus();
          }

          if (helpURL)
          {
            openWindow(helpURL)
          }
          else
          {
            this.getHelpUrl(openWindow);
          }
        },
        showAPI: function()
        {
          var openWindow = function(url)
          {
            var newWindow = window.open(url,'Qpid REST API','height=800,width=800,scrollbars=1,location=1,resizable=1,status=0,toolbar=1,titlebar=1,menubar=1', true);
            newWindow.focus();
          }

          openWindow("/apidocs");
        }

    };

});
