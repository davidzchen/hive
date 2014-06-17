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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hive.hcatalog.mapreduce.FileRecordWriterContainer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * Record writer container for tables using static partitioning. See
 * {@link FileOutputFormatContainer} for more information
 */
class StaticPartitionFileRecordWriterContainer extends FileRecordWriterContainer {
  /**
   * @param baseWriter RecordWriter to contain
   * @param context current TaskAttemptContext
   * @throws IOException
   * @throws InterruptedException
   */
  public StaticPartitionFileRecordWriterContainer(TaskAttemptContext context,
      HiveOutputFormat baseOutputFormat)
      throws IOException, InterruptedException {
    super(context);
    Configuration conf = context.getConfiguration();
    JobConf jobConf = new JobConf(conf);
    Path parentDir = new Path(conf.get("mapred.work.output.dir"));
    Path childPath = new Path(parentDir,
        FileOutputFormat.getUniqueName(jobConf, "part"));

    boolean isCompressed = conf.getBoolean("mapred.output.compress", false);
    Class<? extends Writable> valueClass = null;
    try {
      valueClass = (Class<? extends Writable>)
          Class.forName(conf.get("mapred.output.value.class"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    FileSinkOperator.RecordWriter recordWriter =
        baseOutputFormat.getHiveRecordWriter(
            jobConf, childPath, valueClass, isCompressed, tableProperties,
            InternalUtil.createReporter(context));
    setBaseRecordWriter(recordWriter);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    getBaseRecordWriter().close(false);
  }

  @Override
  protected LocalFileWriter getLocalFileWriter(HCatRecord value) throws IOException, HCatException {
    return new LocalFileWriter(getBaseRecordWriter(), objectInspector, serDe, jobInfo);
  }
}
