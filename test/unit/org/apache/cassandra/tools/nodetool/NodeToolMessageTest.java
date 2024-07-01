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

package org.apache.cassandra.tools.nodetool;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NodeToolMessageTest extends CQLToolRunnerTester
{
    @Test
    public void testCompareHelpCommand()
    {
        List<String> outNodeTool = sliceStdout(invokeNodetoolInJvmV1("help"));
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolInJvmV2("help"));

        String diff = computeDiff(outNodeTool, outNodeToolV2);
        assertTrue(printFormattedDiffsMessage(outNodeTool, outNodeToolV2, "help", diff),
                   StringUtils.isBlank(diff));
    }

    @Test
    public void testBaseCommandOutput()
    {
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolInJvmV1());
        System.out.println(printFormattedNodeToolOutput(outNodeToolV2));
    }
}
