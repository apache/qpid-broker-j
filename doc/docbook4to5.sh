#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

curl -O https://svn.code.sf.net/p/docbook/code/trunk/docbook/relaxng/tools/db4-upgrade.xsl

for i in `find . -name "*.xml" | grep -v commonEntities`
do

   echo $i
   sed -e 's/&\([A-Za-z]*\);/=TODO=\1=/g' < $i > $i.tmp
   cat > $i << EOF
<?xml version="1.0"?>
<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->

EOF
   xsltproc db4-upgrade.xsl $i.tmp | grep -v 'Converted by db4-upgrade version 1.1' | sed -e 's/\(=TODO=\)\([A-Za-z]*\)\(=\)/\&\2;/g' >> $i
done
