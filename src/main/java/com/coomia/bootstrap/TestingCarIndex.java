/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.coomia.bootstrap;

import com.coomia.sink.ElasticsearchSinkFunctionWithConf;
import com.coomia.source.UserEventSource;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * ClassName:Bootstrap Function: TODO ADD FUNCTION. Reason: TODO ADD REASON. Date: 2020年9月9日
 * 下午4:10:09
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class TestingCarIndex {

  public static void main(String[] args) throws Exception {


    String host = "elasticsearch";
    int port = 9200;
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost(host, port, "http"));
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
    String index = "car2020";
    // delete index
    if (client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
      DeleteIndexRequest deleteIndex = new DeleteIndexRequest(index);
      client.indices().delete(deleteIndex, RequestOptions.DEFAULT);
    }
    // mapping config and put
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    {
      builder.startObject("properties");
      {
        builder.startObject("PlateNo");
        {
          builder.field("type", "keyword");
        }
        builder.endObject();

        builder.startObject("id");
        {
          builder.field("type", "keyword");
        }
        builder.endObject();
      }
      builder.endObject();
    }
    builder.endObject();
    // create index car
    CreateIndexRequest createIndex = new CreateIndexRequest(index);
    createIndex.mapping(index, builder);
    createIndex.settings(
        Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 0));
    client.indices().create(createIndex, RequestOptions.DEFAULT);
    client.close();
  }
}
