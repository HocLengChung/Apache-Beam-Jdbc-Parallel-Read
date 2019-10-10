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
package org.devoteam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 *   Example command:
 *   //    mvn compile exec:java -D exec.mainClass=org.devoteam.JdbcParallelRead -D exec.args="--runner=DataflowRunner --project=<YOUR_PROJECT> --region=<YOUR_REGION> --zone=<YOUR_ZONE> --gcpTempLocation=gs://<YOUR_BUCKET>/tmp/"
 */
public class JdbcParallelRead {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcParallelRead.class);
  public static void main(String[] args) throws PropertyVetoException {
      ComboPooledDataSource dataSource = new ComboPooledDataSource();
      dataSource.setDriverClass("com.mysql.cj.jdbc.Driver");
      dataSource.setJdbcUrl("jdbc:mysql://google/<DATABASE_NAME>?cloudSqlInstance=<INSTANCE_CONNECTION_NAME>" +
              "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false" +
              "&user=<MYSQL_USER_NAME>&password=<MYSQL_USER_PASSWORD>");


      dataSource.setMaxPoolSize(10);
      dataSource.setInitialPoolSize(6);
      JdbcIO.DataSourceConfiguration config
              = JdbcIO.DataSourceConfiguration.create(dataSource);

    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());
    String tableName = "HelloWorld";
    int fetchSize = 1000;
//    Create range index chunks Pcollection
    PCollection<KV<String,Iterable<Integer>>> ranges =
            p.apply(String.format("Read from Cloud SQL MySQL: %s",tableName), JdbcIO.<String>read()
            .withDataSourceConfiguration(config)
            .withQuery(String.format("SELECT MAX(`index_id`) from %s", tableName))
            .withRowMapper(new JdbcIO.RowMapper<String>() {
                public String mapRow(ResultSet resultSet) throws Exception {
                    return resultSet.getString(1);
                }
            })
            .withOutputParallelization(false)
            .withCoder(StringUtf8Coder.of()))
            .apply("Distribute", ParDo.of(new DoFn<String, KV<String, Integer>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    int readChunk = fetchSize;
                    int count = Integer.parseInt((String) c.element());
                    int ranges = (int) (count / readChunk);
                    for (int i = 0; i < ranges; i++) {
                        int indexFrom = i * readChunk;
                        int indexTo = (i + 1) * readChunk;
                        String range = String.format("%s,%s",indexFrom, indexTo);
                        KV<String,Integer> kvRange = KV.of(range, 1);
                        c.output(kvRange);
                    }
                    if (count > ranges * readChunk) {
                        int indexFrom = ranges * readChunk;
                        int indexTo = ranges * readChunk + count % readChunk;
                        String range = String.format("%s,%s",indexFrom, indexTo);
                        KV<String,Integer> kvRange = KV.of(range, 1);
                        c.output(kvRange);
                    }
                }
            }))
            .apply("Break Fusion", GroupByKey.create())
    ;


      ranges.apply(String.format("Read ALL %s", tableName), JdbcIO.<KV<String,Iterable<Integer>>,String>readAll()
              .withDataSourceConfiguration(config)
              .withFetchSize(fetchSize)
              .withCoder(StringUtf8Coder.of())
              .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String,Iterable<Integer>>>() {
                  @Override

                  public void setParameters(KV<String,Iterable<Integer>> element,
                                            PreparedStatement preparedStatement) throws Exception {

                      String[] range = element.getKey().split(",");
                      preparedStatement.setInt(1, Integer.parseInt(range[0]));
                      preparedStatement.setInt(2, Integer.parseInt(range[1]));
                  }
              })
                      .withOutputParallelization(false)
              .withQuery(String.format("select * from <DATABASE_NAME>.%s where index_id >= ? and index_id < ?",tableName))
                      .withRowMapper((JdbcIO.RowMapper<String>) resultSet -> {
                          ObjectMapper mapper = new ObjectMapper();
                          ArrayNode arrayNode = mapper.createArrayNode();
                          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                              String columnTypeIntKey ="";
                              try {
                                  ObjectNode objectNode = mapper.createObjectNode();
                                  objectNode.put("column_name",
                                          resultSet.getMetaData().getColumnName(i));

                                  objectNode.put("value",
                                          resultSet.getString(i));
                                  arrayNode.add(objectNode);
                              } catch (Exception e) {
                                  LOG.error("problem columnTypeIntKey: " +  columnTypeIntKey);
                                  throw e;
                              }
                          }
                          return mapper.writeValueAsString(arrayNode);
                      })
              )
              .apply(MapElements.via(
                      new SimpleFunction<String, Integer>() {
                          @Override
                          public Integer apply(String line) {
                              return line.length();
                          }
                      }))
      ;

    p.run();
  }
}

