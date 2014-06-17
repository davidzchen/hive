/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hive.hcatalog.common.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

public class TestHCatExternalDynamicPartitioned extends TestHCatDynamicPartitioned {

  @Override
  protected Map<String, Set<String>> getDisabledStorageFormats() {
    return new HashMap<String, Set<String>>();
  }

  public TestHCatExternalDynamicPartitioned(String storageFormat, String serdeClass,
      String inputFormatClass, String outputFormatClass)
      throws Exception {
    super(storageFormat, serdeClass, inputFormatClass, outputFormatClass);
    tableName = "testHCatExternalDynamicPartitionedTable_" + storageFormat;
    generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
    generateDataColumns();
  }

  @Override
  protected Boolean isTableExternal() {
    return true;
  }

  /**
   * Run the external dynamic partitioning test but with single map task
   * @throws Exception
   */
  @Test
  public void testHCatExternalDynamicCustomLocation() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, getDisabledStorageFormats()));
    runHCatDynamicPartitionedTable(true, "mapred/externalDynamicOutput/${p1}");
  }

}
