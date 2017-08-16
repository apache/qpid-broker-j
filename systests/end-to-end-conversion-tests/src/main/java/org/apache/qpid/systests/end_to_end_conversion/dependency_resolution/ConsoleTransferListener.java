/*
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

package org.apache.qpid.systests.end_to_end_conversion.dependency_resolution;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.aether.transfer.AbstractTransferListener;
import org.eclipse.aether.transfer.MetadataNotFoundException;
import org.eclipse.aether.transfer.TransferEvent;
import org.eclipse.aether.transfer.TransferResource;

public class ConsoleTransferListener extends AbstractTransferListener
{

    private PrintStream _out;

    private Map<TransferResource, Long> _downloads = new ConcurrentHashMap<>();

    private int _lastLength;

    public ConsoleTransferListener()
    {
        this(null);
    }

    public ConsoleTransferListener(PrintStream out)
    {
        this._out = (out != null) ? out : System.out;
    }

    @Override
    public void transferInitiated(TransferEvent event)
    {
        String message = event.getRequestType() == TransferEvent.RequestType.PUT ? "Uploading" : "Downloading";

        _out.println(message + ": " + event.getResource().getRepositoryUrl() + event.getResource()
                                                                                    .getResourceName());
    }

    @Override
    public void transferProgressed(TransferEvent event)
    {
        TransferResource resource = event.getResource();
        _downloads.put(resource, Long.valueOf(event.getTransferredBytes()));

        StringBuilder buffer = new StringBuilder(64);

        for (Map.Entry<TransferResource, Long> entry : _downloads.entrySet())
        {
            long total = entry.getKey().getContentLength();
            long complete = entry.getValue().longValue();

            buffer.append(getStatus(complete, total)).append("  ");
        }

        int pad = _lastLength - buffer.length();
        _lastLength = buffer.length();
        pad(buffer, pad);
        buffer.append('\r');

        _out.print(buffer);
    }

    private String getStatus(long complete, long total)
    {
        if (total >= 1024)
        {
            return toKB(complete) + "/" + toKB(total) + " KB ";
        }
        else if (total >= 0)
        {
            return complete + "/" + total + " B ";
        }
        else if (complete >= 1024)
        {
            return toKB(complete) + " KB ";
        }
        else
        {
            return complete + " B ";
        }
    }

    private void pad(StringBuilder buffer, int spaces)
    {
        String block = "                                        ";
        while (spaces > 0)
        {
            int n = Math.min(spaces, block.length());
            buffer.append(block, 0, n);
            spaces -= n;
        }
    }

    @Override
    public void transferSucceeded(TransferEvent event)
    {
        transferCompleted(event);

        TransferResource resource = event.getResource();
        long contentLength = event.getTransferredBytes();
        if (contentLength >= 0)
        {
            String type = (event.getRequestType() == TransferEvent.RequestType.PUT ? "Uploaded" : "Downloaded");
            String len = contentLength >= 1024 ? toKB(contentLength) + " KB" : contentLength + " B";

            String throughput = "";
            long duration = System.currentTimeMillis() - resource.getTransferStartTime();
            if (duration > 0)
            {
                long bytes = contentLength - resource.getResumeOffset();
                DecimalFormat format = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.ENGLISH));
                double kbPerSec = (bytes / 1024.0) / (duration / 1000.0);
                throughput = " at " + format.format(kbPerSec) + " KB/sec";
            }

            _out.println(type + ": " + resource.getRepositoryUrl() + resource.getResourceName() + " (" + len
                         + throughput + ")");
        }
    }

    @Override
    public void transferFailed(TransferEvent event)
    {
        transferCompleted(event);

        if (!(event.getException() instanceof MetadataNotFoundException))
        {
            event.getException().printStackTrace(_out);
        }
    }

    private void transferCompleted(TransferEvent event)
    {
        _downloads.remove(event.getResource());

        StringBuilder buffer = new StringBuilder(64);
        pad(buffer, _lastLength);
        buffer.append('\r');
        _out.print(buffer);
    }

    public void transferCorrupted(TransferEvent event)
    {
        event.getException().printStackTrace(_out);
    }

    protected long toKB(long bytes)
    {
        return (bytes + 1023) / 1024;
    }
}
