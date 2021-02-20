
/**
 * Project Name:flink-es-sink-demo File Name:Bootstrap.java Package Name:com.coomia.bootstrap
 * Date:2020年9月9日下午4:10:09 Copyright (c) 2020, spancer.ray All Rights Reserved.
 *
 */

package com.coomia.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.coomia.sink.ElasticsearchSinkFunctionWithConf;
import com.coomia.source.UserEventSource;

/**
 * ClassName:Bootstrap Function: TODO ADD FUNCTION. Reason: TODO ADD REASON. Date: 2020年9月9日
 * 下午4:10:09
 * 
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class BootstrapDataProducer {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<String> data = env.addSource(new UserEventSource(3000000));
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("10.116.200.5", 9400, "http"));
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost("10.116.200.5", 9400, "http")));
    String index = "car";
    boolean recreate = false;
    //delete index
     if(recreate && client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT))
     {
       DeleteIndexRequest deleteIndex = new DeleteIndexRequest(index);
       client.indices().delete(deleteIndex, RequestOptions.DEFAULT);
       
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
     }
    ElasticsearchSink.Builder<String> esSinkBuilder =
        new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunctionWithConf(index));
    data.addSink(esSinkBuilder.build());

    env.execute("sink event to es");
  }

}
