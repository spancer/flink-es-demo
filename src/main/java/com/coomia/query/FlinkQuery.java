/**
 * Project Name:flink-es-sink-demo File Name:Bootstrap.java Package Name:com.coomia.bootstrap
 * Date:2020年9月9日下午4:10:09 Copyright (c) 2020, spancer.ray All Rights Reserved.
 */
package com.coomia.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ClassName:Bootstrap Function: TODO ADD FUNCTION. Reason: TODO ADD REASON. Date: 2020年9月9日
 * 下午4:10:09
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class FlinkQuery {

  public static void main(String[] args) throws Exception {

    /** flink环境 */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /** ES环境 */
    String host = "10.116.200.5";
    int port = 9400;
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost(host, port, "http"));
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
    /** 碰撞分析 查询两个区域的车辆交集 */
    String index = "car";
    long start = 1580077247735L; // time start in mills
    long end = 1599255299742L; // time end in mills
    Integer[] areaA = new Integer[] {0, 1, 3, 4, 5, 6}; // deviceIDs of areaA
    Integer[] areaB = new Integer[] {7, 8, 9, 10, 11}; // deviceIDs of areaB

    /** query devices in areaA & areaB, along with time range in start & end. */
    QueryBuilder orQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termsQuery("DeviceID", areaA))
            .should(QueryBuilders.termsQuery("DeviceID", areaB));
    QueryBuilder query =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end))
            .must(orQuery);
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    ssb.query(query);
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices(index); // index name
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHits hits = sr.getHits();

    env.execute("sink event to es");
    client.close();
  }
}
