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
define(["qpid/common/util",
    "dojo/query",
    "dojo/domReady!"],
  function (util, query)
  {
    function CategoryTabExtension(containerNode, template, attributesNode, metadata, data, typeBaseFolder)
    {
      var that = this;
      this.base = typeBaseFolder;
      this.metadata = metadata;
      util.parse(containerNode, template,
        function()
        {
          that.typeSpecificAttributesContainer = query("." + attributesNode, containerNode)[0];
          that.postParse(containerNode);
          that.update(data)
        });
    }

    CategoryTabExtension.prototype.postParse = function (containerNode)
    {
        // no-op
    }

    CategoryTabExtension.prototype.update = function (restData)
    {
      var data = restData || {};
      if (!this.details)
      {
        if (data.type)
        {
          var that = this;
          require([this.base + data.type.toLowerCase() + "/show"],
            function(Details)
            {
              that.details = new Details({containerNode:that.typeSpecificAttributesContainer, metadata: that.metadata, data:data});
            }
          );
        }
      }
      else
      {
        this.details.update(data);
      }
    }

    return CategoryTabExtension;
  }
);
