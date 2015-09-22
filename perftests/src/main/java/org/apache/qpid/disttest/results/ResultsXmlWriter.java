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

package org.apache.qpid.disttest.results;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.results.aggregation.ITestResult;
import org.apache.qpid.disttest.results.aggregation.TestResultAggregator;

/**
 * Simple junit style xml output for ease of incorporation of the results into Jenkins
 */
public class ResultsXmlWriter implements  ResultsWriter
{
    private final DocumentBuilderFactory _docFactory = DocumentBuilderFactory.newInstance();
    private final TransformerFactory _transformerFactory = TransformerFactory.newInstance();
    private final File _outputDir;

    public ResultsXmlWriter(final File outputDir)
    {
        _outputDir = outputDir;
    }

    @Override
    public void begin()
    {

    }

    @Override
    public void writeResults(final ResultsForAllTests resultsForAllTests, final String testConfigFile)
    {
        final String outputFile = generateOutputCsvNameFrom(testConfigFile);
        writeResultsToOutputFile(resultsForAllTests, testConfigFile, outputFile);

    }

    @Override
    public void end()
    {

    }

    private void writeResultsToOutputFile(final ResultsForAllTests resultsForAllTests,
                                          final String inputFile,
                                          final String outputFile)
    {
        try
        {
            DocumentBuilder docBuilder = _docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            doc.setXmlStandalone(true);

            Element root = doc.createElement("testsuite");
            root.setAttribute("tests", Integer.toString(resultsForAllTests.getTestResults().size()));

            doc.appendChild(root);

            for(ITestResult testResult : resultsForAllTests.getTestResults())
            {
                Element testcase = doc.createElement("testcase");
                testcase.setAttribute("classname", inputFile);
                testcase.setAttribute("name", testResult.getName());
                long timeTaken = getTimeTaken(testResult);
                if (timeTaken > 0)
                {
                    testcase.setAttribute("time", String.valueOf(timeTaken / 1000));
                }

                if (testResult.hasErrors())
                {
                    for (ParticipantResult result : testResult.getParticipantResults())
                    {
                        if (result.hasError())
                        {
                            Element error = doc.createElement("error");
                            error.setAttribute("message", result.getErrorMessage());
                            testcase.appendChild(error);
                        }
                    }
                }
                root.appendChild(testcase);
            }

            Transformer transformer = _transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(outputFile));
            transformer.transform(source, result);

        }
        catch (ParserConfigurationException | TransformerException e)
        {
            throw new DistributedTestException("Failed to create xml file : " + outputFile, e);
        }
    }

    private long getTimeTaken(final ITestResult testResult)
    {
        for(ParticipantResult result : testResult.getParticipantResults())
        {
            if (TestResultAggregator.ALL_PARTICIPANTS_NAME.equals(result.getParticipantName()))
            {
                return result.getTimeTaken();
            }
        }
        return 0;
    }

    private String generateOutputCsvNameFrom(String testConfigFile)
    {
        final String filenameOnlyWithExtension = new File(testConfigFile).getName();
        final String xmlFile = filenameOnlyWithExtension.replaceFirst(".?\\w*$", ".xml");

        return new File(_outputDir, xmlFile).getAbsolutePath();
    }

}
