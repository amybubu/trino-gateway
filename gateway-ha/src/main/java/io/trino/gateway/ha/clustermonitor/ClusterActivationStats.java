/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.gateway.ha.clustermonitor;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.HaGatewayManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClusterActivationStats
{
    private final GatewayBackendManager gatewayBackendManager;
    private final Map<String, Integer> clusterStatusCache = new ConcurrentHashMap<>();
    private static final Logger log = Logger.get(HaGatewayManager.class);

    @Inject
    public ClusterActivationStats(GatewayBackendManager gatewayBackendManager)
    {
        this.gatewayBackendManager = gatewayBackendManager;
    }

//    public void initActivationStatusMetrics()
//    {
//        List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();
//        for (ProxyBackendConfiguration backend : backends) {
//            String clusterId = backend.getName();
//            createActivationStatusMetric(clusterId);
//        }
//    }

//    public void initActivationStatusMetricByCluster(String clusterName)
//    {
//        createActivationStatusMetric(clusterName);
//    }

//    public int createActivationStatusMetric(String clusterName)
//    {
//        return gatewayBackendManager.getBackendByName(clusterName)
//                .map(backend -> backend.isActive() ? 1 : 0)
//                .orElse(-1);
//    }

    public void initActivationStatusMetrics()
    {
        log.info("INSIDE initActivationStatusMetrics");
        System.out.println("INSIDE initActivationStatusMetrics");
        List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();
        log.info("AMY LOG backends = %s", backends);
        for (ProxyBackendConfiguration backend : backends) {
            String clusterId = backend.getName();
            log.info("AMY LOG clusterId = %s", clusterId);
            //getClusterActivationStatusMetric(clusterId);
            clusterStatusCache.put(clusterId, getClusterActivationStatusMetric(clusterId));
            log.info("initActivationStatusMetrics CACHE: %s", clusterStatusCache);
        }
        log.info("Cache after population: %s", clusterStatusCache);
    }

    public void initActivationStatusMetricByCluster(String clusterName)
    {
        log.info("CALLING initActivationStatusMetricByCluster: %s", clusterName);
        System.out.println("INSIDE initActivationStatusMetricByCluster");
        clusterStatusCache.put(clusterName, getClusterActivationStatusMetric(clusterName));
        log.info("initActivationStatusMetricByCluster CACHE: %s", clusterStatusCache);
    }

    private int getClusterActivationStatusMetric(String clusterName)
    {
        log.info("CALLING getClusterActivationStatusMetric(: %s", clusterName);
        System.out.println("INSIDE getClusterActivationStatusMetric(");
        int activationStatus = gatewayBackendManager.getBackendByName(clusterName)
                .map(backend -> backend.isActive() ? 1 : 0)
                .orElse(-1);
        log.info("AMY LOG %s activationStatus: %s", clusterName, activationStatus);
        return activationStatus;
    }

//    @Managed
//    @Nested
//    public Map<String, Integer> getClusterActivationMetrics()
//    {
//        log.info("Returning ClusterActivationMetrics: %s", clusterStatusCache);
//        System.out.println("INSIDE getClusterActivationMetrics");
//        return clusterStatusCache.entrySet().stream()
//                .collect(Collectors.toMap(Map.Entry::getKey, e -> getClusterActivationStatusMetric(e.getKey())));
//    }

    @Managed
    @Nested
    public List<String> getMetricsCache()
    {
        System.out.println("INSIDE getMetricsCache");
        log.info("getMetricsCache() called at %s", System.currentTimeMillis());
        log.info("Returning ClusterActivationMetrics: %s", clusterStatusCache);
        return clusterStatusCache.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.toList());
    }
}
