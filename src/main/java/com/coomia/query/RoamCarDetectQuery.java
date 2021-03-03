/**
 * Project Name:flink-es-sink-demo File Name:ESQueryDemo.java Package Name:com.coomia.query
 * Date:2020年9月9日下午5:25:37 Copyright (c) 2020, spancer.ray All Rights Reserved.
 */
package com.coomia.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 车辆徘徊查询
 *
 * <p>where shotTime between $t1 and $t2 and DeviceID in($d1,$d2, $d3...) group by DeviceID, PlateNo
 * having count(plateNo)>=3
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class RoamCarDetectQuery {

  public static void main(String[] args) throws IOException {

    long start = 1580077247735L;
    long end = 1599255299742L;
    int[] areaA = new int[] {1, 5, 6, 8};
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost("10.116.200.5", 9400, "http")));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    QueryBuilder query =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end))
            .must(QueryBuilders.termsQuery("DeviceID", areaA));
    ssb.query(query);
    TermsAggregationBuilder parentAgg =
        AggregationBuilders.terms("GroupbyDevice").field("DeviceID");
    Map<String, String> bucketsPathsMap = new HashMap<String, String>();
    bucketsPathsMap.put("plateNo_count", "_count");

    Map<String, Object> havingScriptParam = new HashMap<String, Object>();
    havingScriptParam.put("havingCount", 3);
    Script script =
        new Script(
            ScriptType.INLINE, "expression", "plateNo_count >= havingCount", havingScriptParam);

    TermsAggregationBuilder childAgg =
        AggregationBuilders.terms("GroupbyPlateNo")
            .field("PlateNo")
            .subAggregations(
                AggregatorFactories.builder()
                    .addPipelineAggregator(
                        PipelineAggregatorBuilders.bucketSelector(
                            "HavingPlateNoGT1", bucketsPathsMap, script)));
    parentAgg.subAggregation(childAgg);
    ssb.size(0);
    ssb.aggregation(parentAgg);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("car");
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    long took = sr.getTook().getMillis();
    System.out.println("耗时：" + took);

    Aggregations aggRes = sr.getAggregations();
    Terms devices = aggRes.get("GroupbyDevice");
    List<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> data =
        devices.getBuckets();
    for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bks : data) {
      org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms aggs =
          bks.getAggregations().get("GroupbyPlateNo");
      for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket :
          aggs.getBuckets()) {
        System.out.println(
            "Device: "
                + bks.getKey()
                + " Count: "
                + bks.getDocCount()
                + " PlateNo: "
                + bucket.getKeyAsString()
                + "  Count : "
                + bucket.getDocCount());
      }
    }
    client.close();
  }
}
