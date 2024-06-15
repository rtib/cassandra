/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertTrue;

public class NodeToolSynopsisTest
{
    private static final Map<String, String> NODETOOLV2_ENV = ImmutableMap.of("NODETOOL_RUNNER", "org.apache.cassandra.tools.NodeToolV2");

    @Test
    public void cliHelpForcecompact()
    {
        ToolResult toolHistory = invokeNodetool(List.of("help", "forcecompact"));
        toolHistory.assertOnCleanExit();
        String outputV1 = toolHistory.getStdout();

        toolHistory = invokeNodetool(NODETOOLV2_ENV, List.of("help", "forcecompact"));
        String outputV2 = toolHistory.getStdout();
    }

    @Test
    public void cliHelp()
    {
        ToolResult toolHistory = invokeNodetool("help");
        toolHistory.assertOnCleanExit();
        String outputV1 = toolHistory.getStdout();

        toolHistory = invokeNodetool(NODETOOLV2_ENV, List.of("help"));
        String outputV2 = toolHistory.getStdout();
    }

    @Test
    public void cliDryRun() throws Exception
    {
        List<String> args = CQLTester.buildNodetoolArgs(List.of("help", "assassinate"));
        args.remove("bin/nodetool");
        ListOutputStream outputV1 = new ListOutputStream();
        ListOutputStream outputV2 = new ListOutputStream();

        new NodeTool(new NodeProbeFactory(), new Output(new PrintStream(outputV1), new PrintStream(outputV1)))
            .execute(args.toArray(new String[0]));
        new NodeToolV2(new NodeProbeFactory(), new Output(new PrintStream(outputV2), new PrintStream(outputV2)))
            .execute(args.toArray(new String[0]));

        String diff = computeDiff(outputV1.getOutputLines(), outputV2.getOutputLines());
        assertTrue(String.join("\n", outputV1.getOutputLines()) +
                   '\n' + String.join("\n", outputV2.getOutputLines()) +
                   '\n' + " difference: " + diff,
                   StringUtils.isBlank(diff));
    }

    public static String computeDiff(List<String> original, List<String> revised) {
        Patch<String> patch = DiffUtils.diff(original, revised);
        List<String> diffLines = new ArrayList<>();

        for (AbstractDelta<String> delta : patch.getDeltas()) {
            for (String line : delta.getSource().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " source: " + line);
            }
            for (String line : delta.getTarget().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " target: " + line);
            }
        }

        return '\n' + String.join("\n", diffLines);
    }

    private static class ListOutputStream extends OutputStream
    {
        private final List<String> outputLines = new ArrayList<>();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void write(int b)
        {
            char c = (char) b;
            if (c == '\n')
            {
                // Add the buffer to the list if it's a new line
                outputLines.add(buffer.toString());
                buffer.setLength(0); // Clear the buffer
            }
            else
                buffer.append(c);
        }

        public void flush()
        {
            if (buffer.length() > 0)
            {
                outputLines.add(buffer.toString());
                buffer.setLength(0);
            }
        }

        public List<String> getOutputLines()
        {
            flush();
            return new ArrayList<>(outputLines);
        }
    }
}
