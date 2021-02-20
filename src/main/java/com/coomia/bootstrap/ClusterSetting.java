
/**
 * Project Name:flink-es-sink-demo File Name:Bootstrap.java Package Name:com.coomia.bootstrap
 * Date:2020年9月9日下午4:10:09 Copyright (c) 2020, spancer.ray All Rights Reserved.
 *
 */

package com.coomia.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

/**
 * ClassName:Bootstrap Function: TODO ADD FUNCTION. Reason: TODO ADD REASON. Date: 2020年9月9日
 * 下午4:10:09
 * 
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class ClusterSetting {

  public static void main(String[] args) throws Exception {

    String host = "10.116.200.5";
    int port = 9400;
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost(host, port, "http"));
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
    ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
    Settings persistentSettings =
        Settings.builder().put("script.max_compilations_rate", "100/1m").build();
    request.persistentSettings(persistentSettings);
    client.close();
  }

}

