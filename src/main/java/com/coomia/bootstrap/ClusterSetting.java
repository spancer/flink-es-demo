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

import java.util.ArrayList;
import java.util.List;
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
