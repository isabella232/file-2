/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * File Sink
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(FileSink.NAME)
@Description("Batch File Sink that can be used to write to any HDFS Compatible file system.")
public class FileSink extends BatchSink<StructuredRecord, Text, NullWritable> {
  private static final String NULL_STRING = "\0";
  @VisibleForTesting
  static final String NAME = "File";

  private FileSinkConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // if user provided macro, need to still validate timeSuffix format
    config.validate();
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(config, context)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, NullWritable>> emitter) throws Exception {
    List<String> dataArray = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      Object fieldValue = input.get(field.getName());
      String data = (fieldValue != null) ? fieldValue.toString() : NULL_STRING;
      dataArray.add(data);
    }
    emitter.emit(new KeyValue<>(new Text(Joiner.on(config.delimiter).join(dataArray)), NullWritable.get()));
  }

  /**
   * File Sink Output Provider.
   */
  public static class SinkOutputFormatProvider implements OutputFormatProvider {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    private final Map<String, String> conf;

    SinkOutputFormatProvider(FileSinkConfig config, BatchSinkContext context) {
      this.conf = new HashMap<>();
      String timeSuffix = !Strings.isNullOrEmpty(config.timeSufix) ?
        new SimpleDateFormat(config.timeSufix).format(context.getLogicalStartTime()) : "";
      conf.put(FileOutputFormat.OUTDIR, String.format("%s/%s", config.path, timeSuffix));
      if (!Strings.isNullOrEmpty(config.jobProperties)) {
        Map<String, String> arguments = GSON.fromJson(config.jobProperties, MAP_TYPE);
        for (Map.Entry<String, String> argument : arguments.entrySet()) {
          conf.put(argument.getKey(), argument.getValue());
        }
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return TextOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config for File Sink.
   */
  static class FileSinkConfig extends PluginConfig {

    @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
    private String referenceName;

    @Name("path")
    @Description("Absolute file path of the output directory. e.g. 'hdfs://mycluster.net:8020/output', " +
      "gs://mybucket/path/to/output")
    @Macro
    private String path;

    @Name("suffix")
    @Description("Time Suffix used for destination directory for each run. For example, 'YYYY-MM-dd-HH-mm'. " +
      "By default, no time suffix is used.")
    @Nullable
    @Macro
    private String timeSufix;

    @Name("jobProperties")
    @Nullable
    @Description("Advanced feature to specify any additional properties that should be used with the sink, " +
      "specified as a JSON object of string to string. These properties are set on the job.")
    @Macro
    private String jobProperties;

    @Nullable
    @Description("The delimiter to use when concatenating record fields. Defaults to a comma (',').")
    @Macro
    private String delimiter;

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
    private static final Pattern DATASET_ID_PATTERN = Pattern.compile("[$\\.a-zA-Z0-9_-]+");

    private FileSinkConfig() {
      jobProperties = "{}";
      delimiter = ",";
    }

    private void validate() {
      if (!DATASET_ID_PATTERN.matcher(referenceName).matches()) {
        throw new IllegalArgumentException(String.format("%s is not a valid id. Allowed characters are letters, " +
                                                           "numbers, and _, -, ., or $.", referenceName));
      }
      // if macro provided, timeSuffix will be null at configure time
      if (!Strings.isNullOrEmpty(timeSufix)) {
        new SimpleDateFormat(timeSufix);
      }
      if (!Strings.isNullOrEmpty(jobProperties)) {
        // Try to parse the JSON and propagate the error
        GSON.fromJson(jobProperties, MAP_STRING_STRING_TYPE);
      }
    }
  }
}
