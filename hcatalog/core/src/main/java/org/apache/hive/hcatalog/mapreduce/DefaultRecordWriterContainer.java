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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * Part of the DefaultOutput*Container classes
 * See {@link DefaultOutputFormatContainer} for more information
 */
class DefaultRecordWriterContainer extends RecordWriterContainer {

  private final HiveStorageHandler storageHandler;
  private final SerDe serDe;
  private final OutputJobInfo jobInfo;
  private final ObjectInspector hcatRecordOI;

  /**
   * @param context current JobContext
   * @throws IOException
   * @throws InterruptedException
   */
  public DefaultRecordWriterContainer(TaskAttemptContext context,
      HiveOutputFormat baseOutputFormat) throws IOException {
    super(context);
    Configuration conf = context.getConfiguration();
    Properties tableProperties = new Properties();

    jobInfo = HCatOutputFormat.getJobInfo(conf);
    storageHandler = HCatUtil.getStorageHandler(conf, jobInfo.getTableInfo().getStorerInfo());
    HCatOutputFormat.configureOutputStorageHandler(context);
    serDe = ReflectionUtils.newInstance(storageHandler.getSerDeClass(), conf);
    hcatRecordOI = InternalUtil.createStructObjectInspector(jobInfo.getOutputSchema());
    try {
      InternalUtil.initializeOutputSerDe(serDe, tableProperties, context.getConfiguration(),
          jobInfo);
    } catch (SerDeException e) {
      throw new IOException("Failed to initialize SerDe", e);
    }

    // Initialize RecordWriter.
    Path parentDir = new Path(conf.get("mapred.work.output.dir"));
    Path childPath = new Path(parentDir,
        FileOutputFormat.getUniqueFile(context, "part", ""));

    boolean isCompressed = conf.getBoolean("mapred.output.compress", false);
    Class<? extends Writable> valueClass = null;
    try {
      valueClass = (Class<? extends Writable>)
          Class.forName(conf.get("mapred.output.value.class"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    FileSinkOperator.RecordWriter recordWriter = baseOutputFormat.getHiveRecordWriter(
        new JobConf(conf), childPath, valueClass, isCompressed, tableProperties,
        InternalUtil.createReporter(context));
    setBaseRecordWriter(recordWriter);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    getBaseRecordWriter().close(false);
  }

  @Override
  public void write(WritableComparable<?> key, HCatRecord value) throws IOException,
    InterruptedException {
    try {
      getBaseRecordWriter().write(serDe.serialize(value.getAll(), hcatRecordOI));
    } catch (SerDeException e) {
      throw new IOException("Failed to serialize object", e);
    }
  }

}
