/**
 * Project Name:flink-es-sink-demo File Name:DeckCarDetectQuery.java Package Name:com.coomia.query
 * Date:2020年9月15日上午9:02:49 Copyright (c) 2020, spancer.ray All Rights Reserved.
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ClassName:DeckCarDetectQuery Function: 通过套牌车辆分析，可及时掌握套牌情况，加强对公共安全的管控。
 * 不同车身、不同车型、不同品牌、不同区域(deviceID)同时出现(within time interval) Date: 2020年9月15日 上午9:02:49
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class DeckCarDetectQuery {

  public static void main(String[] args) throws IOException {

    long start = 1580077247735L;
    long end = 1600134952522L;
    String plateNo = null;
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost("10.116.200.5", 9400, "http")));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    BoolQueryBuilder query =
        QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end));
    if (null != plateNo) query.must(QueryBuilders.termsQuery("PlateNo", plateNo));
    CardinalityAggregationBuilder color =
        AggregationBuilders.cardinality("plateColorDescDistinct").field("plateColorDesc");
    CardinalityAggregationBuilder clas =
        AggregationBuilders.cardinality("vehicleClassDescDistinct").field("vehicleClassDesc");
    CardinalityAggregationBuilder brand =
        AggregationBuilders.cardinality("VehicleBrandDistinct").field("VehicleBrand");
    Script intervalInit = new Script("state.a1 = []");
    Script intervalMap =
        new Script(
            "for(int i=0; i< doc['location'].length; i++) { doc['location'][i].arcDistance(\"doc['bayonetLatitude'][i]\"+\",\"+\"doc['bayonetLongitude'][i]\")}");
    Script intervalCombine = new Script("");
    Script intervalReduce = new Script("");
    ScriptedMetricAggregationBuilder interval = AggregationBuilders.scriptedMetric("Interval");
    interval.initScript(intervalInit);
    interval.mapScript(intervalMap);
    interval.combineScript(intervalCombine);
    interval.reduceScript(intervalReduce);
    /** build script and params. */
    Map<String, String> bucketsPathsMap = new HashMap<String, String>();
    bucketsPathsMap.put("plateColorDescDistinct", "plateColorDescDistinct");
    bucketsPathsMap.put("vehicleClassDescDistinct", "vehicleClassDescDistinct");
    bucketsPathsMap.put("VehicleBrandDistinct", "VehicleBrandDistinct");
    Map<String, Object> havingScriptParam = new HashMap<String, Object>();
    havingScriptParam.put("havingCount", 1);
    Script script =
        new Script(
            ScriptType.INLINE,
            "expression",
            "plateColorDescDistinct >havingCount || vehicleClassDescDistinct >havingCount || VehicleBrandDistinct > havingCount",
            havingScriptParam);
    BucketSelectorPipelineAggregationBuilder having =
        PipelineAggregatorBuilders.bucketSelector("HavingPlateNoGT1", bucketsPathsMap, script);

    ssb.aggregation(
        AggregationBuilders.terms("GroupbyPlateNo")
            .field("PlateNo")
            .subAggregation(color)
            .subAggregation(clas)
            .subAggregation(brand)
            .subAggregation(interval)
            .subAggregation(having));
    ssb.query(query);
    ssb.size(0);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("car");
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    long took = sr.getTook().getMillis();
    System.out.println("耗时：" + took);

    Aggregations aggRes = sr.getAggregations();
    Terms devices = aggRes.get("GroupbyPlateNo");
    List<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> data =
        devices.getBuckets();
    for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bks : data) {
      bks.getAggregations().get("plateColorDescDistinct");
      bks.getAggregations().get("vehicleClassDescDistinct");
      System.out.println(
          "PlateNo: " + bks.getKey() + " Count: " + bks.getDocCount() + " PlateNo: ");
    }

    client.close();
  }
}
