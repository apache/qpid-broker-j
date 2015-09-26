#!/usr/bin/env python
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


import sys
import os
import tempfile
import subprocess
import re

(COLUMN_HEAP,
 COLUMN_DIRECT,
 COLUMN_CONNECTIONS,
 COLUMN_SESSIONS,
 COLUMN_PRODUCERS,
 COLUMN_TRANSACTED,
 COLUMN_MESSAGES,
 COLUMN_MESSAGE_SIZE,
 COLUMN_DELIVERY_MODE,
 COLUMN_HUMAN_READABLE_HEAP,
 COLUMN_HUMAN_READABLE_DIRECT) = range(11)


TEMPLATE = """
set datafile separator ","
set terminal png
set output "{outputFile}"

byte_buffer_size=256*1024

heap(messages, connections, sessions)=15*1024*1024 + 15*1024*sessions*connections + 1024*messages + 17*1024*connections
direct(messages, messageSize, connections, sessions)=2*1024*1024 + (200+messageSize*2)*messages + 2*byte_buffer_size*connections

set title "{title}"
set key top left
set log xy

set xlabel "{xlabel}"
set ylabel "Memory Usage in MBytes"

plot {plotCmdArgs}
"""


def main():
    if len(sys.argv) != 2:
        printUsage()
        sys.exit(1)

    filename = sys.argv[1]

    if not os.path.exists(filename):
        print "file " + filename + " doesn't exist"

    protocol, jvm, data = parseCSV(filename)
    print protocol, jvm
    print("creating plots for datafile '%s'" % filename)
    plotForProtocol(data, protocol, jvm, TEMPLATE)


def printUsage():
    print "Usage: {scriptName} RESULT_FILE".format(scriptName = sys.argv[0])


def parseCSV(filename):
    rows = []
    protocolVersion = None
    javaVersion = None
    with open(filename) as f:
        lastLine = None
        for line in f:
            if line.startswith("#"):
                # parse header for amqp and java version
                if protocolVersion == None or javaVersion == None:
                    protocolMatch = re.search("amqp=(\d-\d+(?:-\d+)?)", line)
                    protocolVersion = protocolMatch.group(1) if protocolMatch else None
                    javaMatch = re.search("java=(1\.\d)", line)
                    javaVersion = javaMatch.group(1) if javaMatch else None
                # if not header a comment line indicates an execption of the previous execution so delete that result
                if rows and not lastLine.startswith("#"):
                    rows = rows[:-1]
                lastLine = line
                continue
            columns = line.strip().split(",")
            rows.append(columns)
            lastLine = line
    return (protocolVersion, javaVersion, rows)

def plotForProtocol(data, protocol, jvm, template):
    print("  creating plots for " + protocol)
    print("    creating by_messages plots...")
    for messageSize in (0, 1024, 102400, 10485760, 32768000):
        plotCmdArgs = ('datafile using 7:($1/1024/1024) title "Heap Memory",'
                       '(heap(x,1,1)/1024/1024) title "heap formula" linecolor rgb "red",'
                       'datafile using 7:($2/1024/1024) title "Direct Memory" linecolor rgb "blue",'
                       '(direct(x,%s,1,1)/1024/1024) title "direct formual" linecolor rgb "blue"' % messageSize)
        plotScript = template.format(title="Memory Usage by Number of Messages\\n(AMQP=%s; messageSize=%s)" % (protocol, messageSize),
                                     xlabel="Number of Messages",
                                     outputFile="memUsage_by_messages_%s_msgSize=%s.png" % (protocol, messageSize),
                                     plotCmdArgs=plotCmdArgs)
        plotDataPredicate = lambda line: (line[COLUMN_CONNECTIONS]=="1" and line[COLUMN_SESSIONS]=="1" and
                                          line[COLUMN_PRODUCERS]=="1" and line[COLUMN_MESSAGE_SIZE]=="%s" % messageSize)
        plotByPredicate(plotScript, data, plotDataPredicate)

    print("    creating by_msgSize plots...")
    for messages in (10, 50, 100, 1000, 10000, 100000):
        plotCmdArgs = ('datafile using 8:($1/1024/1024) title "Heap Memory",'
                       '(heap(%s,1,1)/1024/1024) title "heap formula" linecolor rgb "red",'
                       'datafile using 8:($2/1024/1024) title "Direct Memory" linecolor rgb "blue",'
                       '(direct(%s,x,1,1)/1024/1024) title "direct formual" linecolor rgb "blue"' % (messages, messages))
        plotScript = template.format(title="Memory Usage by MessageSize\\n(AMQP=%s; messages=%s)" % (protocol, messages),
                                     xlabel="Message Size",
                                     outputFile="memUsage_by_msgSize_%s_msgNum=%s.png" % (protocol, messages),
                                     plotCmdArgs=plotCmdArgs)
        plotDataPredicate = lambda line: (line[COLUMN_CONNECTIONS]=="1" and line[COLUMN_SESSIONS]=="1" and
                                          line[COLUMN_PRODUCERS]=="1" and line[COLUMN_MESSAGES]=="%s" % messages)
        plotByPredicate(plotScript, data, plotDataPredicate)

    print("    creating by_connections plots...")
    for sessions in (10, 100, 200):
        plotCmdArgs = ('datafile using 3:($1/1024/1024) title "Heap Memory",'
                       '(heap(2,x,%s)/1024/1024) title "heap formula" linecolor rgb "red",'
                       'datafile using 3:($2/1024/1024) title "Direct Memory" linecolor rgb "blue",'
                       '(direct(2,0,x,%s)/1024/1024) title "direct formual" linecolor rgb "blue"' % (sessions, sessions))
        plotScript = template.format(title="Memory Usage by Connections\\n(AMQP=%s; sessions=%s)" % (protocol, sessions),
                                     xlabel="Number of Connections",
                                     outputFile="memUsage_by_connections_%s_sessions=%s.png" % (protocol, sessions),
                                     plotCmdArgs=plotCmdArgs)
        plotDataPredicate = lambda line: (line[COLUMN_SESSIONS]=="%s" % sessions and line[COLUMN_PRODUCERS]=="1" and
                                          line[COLUMN_MESSAGES]=="2" and line[COLUMN_MESSAGE_SIZE]=="0")
        plotByPredicate(plotScript, data, plotDataPredicate)

    print("    creating by_sessions plots...")
    for connections in (1, 100, 1000):
        plotCmdArgs = ('datafile using 4:($1/1024/1024) title "Heap Memory",'
                       '(heap(2,%s,x)/1024/1024) title "heap formula" linecolor rgb "red",'
                       'datafile using 4:($2/1024/1024) title "Direct Memory" linecolor rgb "blue",'
                       '(direct(2,0,%s,x)/1024/1024) title "direct formual" linecolor rgb "blue"' % (connections,connections))
        plotScript = template.format(title="Memory Usage by Sessions per Connection\\n(AMQP=%s; connections=%s)" % (protocol, connections),
                                     xlabel="Number of Sessions per Connection",
                                     outputFile="memUsage_by_sessions_%s_connections=%s.png" % (protocol, connections),
                                     plotCmdArgs=plotCmdArgs)
        plotDataPredicate = lambda line: (line[COLUMN_CONNECTIONS]=="%s" % connections and line[COLUMN_PRODUCERS]=="1" and
                                          line[COLUMN_MESSAGES]=="2" and line[COLUMN_MESSAGE_SIZE]=="0")
        plotByPredicate(plotScript, data, plotDataPredicate)


def plotByPredicate(plotScript, data, predicate):
    filteredData = []
    for line in data:
        if predicate(line):
            filteredData.append(line)
    plot(plotScript, filteredData)

def plot(plotScript, data):
    plotFile = tempfile.NamedTemporaryFile(mode="w")
    plotFile.write(plotScript)
    plotFile.flush()
    tmpDataFile = tempfile.NamedTemporaryFile(mode="w")
    for line in data:
        tmpDataFile.write(",".join(line) + "\n")
    tmpDataFile.flush()
    subprocess.call(["gnuplot",
                      "-e",
                      "datafile=\"%s\"" % tmpDataFile.name,
                      plotFile.name],
                      stdout=sys.stdout, stderr=sys.stderr)

if __name__ == '__main__':
    main()

