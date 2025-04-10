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
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Singleton
public class BackendsMetricStats
{
    private static final Logger log = Logger.get(BackendsMetricStats.class);

    private final Map<String, BackendClusterMetricStats> statsMap = new ConcurrentHashMap<>();
    private final MBeanExporter exporter;
    private final GatewayBackendManager gatewayBackendManager;

    @Inject
    public BackendsMetricStats(GatewayBackendManager gatewayBackendManager, MBeanExporter exporter)
    {
        this.gatewayBackendManager = gatewayBackendManager;
        this.exporter = exporter;
    }

    public void init()
    {
        // Get current backends from DB
        List<ProxyBackendConfiguration> currentBackends = gatewayBackendManager.getAllBackends();
        Set<String> currentBackendNames = currentBackends
                .stream()
                .map(ProxyBackendConfiguration::getName)
                .collect(Collectors.toSet());

        // Remove metrics for backends that no longer exist
        Set<String> staleClusters = statsMap.keySet()
                .stream()
                .filter(name -> !currentBackendNames.contains(name))
                .collect(Collectors.toSet());

        for (String staleCluster : staleClusters) {
            unregisterBackendMetrics(staleCluster);
        }

        // Register metrics for current backends
        for (ProxyBackendConfiguration backend : gatewayBackendManager.getAllBackends()) {
            registerBackendMetrics(backend);
        }
    }

    public void registerBackendMetrics(ProxyBackendConfiguration backend)
    {
        String clusterName = backend.getName();
        BackendClusterMetricStats stats = new BackendClusterMetricStats(clusterName, gatewayBackendManager);

        if (statsMap.putIfAbsent(clusterName, stats) == null) {  // null means the stats didn't exist previously and was inserted
            try {
                exporter.exportWithGeneratedName(stats, BackendClusterMetricStats.class, clusterName);
                log.info("Registered metrics for cluster: %s", clusterName);
            }
            catch (Exception e) {
                statsMap.remove(clusterName);
                log.error(e, "Failed to register metrics for cluster: %s", clusterName);
            }
        }
    }

    public void unregisterBackendMetrics(String clusterName)
    {
        statsMap.computeIfPresent(clusterName, (name, stats) -> {
            try {
                exporter.unexportWithGeneratedName(BackendClusterMetricStats.class, name);
                log.info("Unregistered metrics for cluster: %s", name);
                return null; // Returning null removes the entry from the map
            }
            catch (Exception e) {
                log.error(e, "Failed to unregister metrics for cluster: %s", name);
                return stats; // Keeps the entry in the map if unregistration fails
            }
        });
    }

    @Managed
    public void refreshOnJmxAccess()
    {
        log.info("JMX endpoint accessed, refreshing backend metrics");
        init();
    }
}
