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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.weakref.jmx.Nested;


@Singleton
public class BackendsMetricStats
{
    private static final Logger log = Logger.get(BackendsMetricStats.class);

    private final MBeanExporter exporter;
    private final GatewayBackendManager gatewayBackendManager;
    private Map<String, BackendClusterMetricStats> statsMap = new ConcurrentHashMap<>();

    @Inject
    public BackendsMetricStats(GatewayBackendManager gatewayBackendManager, MBeanExporter exporter)
    {
        this.gatewayBackendManager = gatewayBackendManager;
        this.exporter = exporter;
    }

    public void init()
    {
        // Unregister all backend metrics
        for (BackendClusterMetricStats stats : statsMap.values()) {
            String name = stats.getClusterName();
            try {
                exporter.unexportWithGeneratedName(BackendClusterMetricStats.class, name);
                log.info("Unregistered metrics for cluster: %s", name);
            }
            catch (Exception e) {
                log.error(e, "Failed to unregister metrics for cluster: %s", name);
            }
        }

        statsMap = new ConcurrentHashMap<>();
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

    @Managed
    public void refreshOnJmxAccess()
    {
        log.info("JMX endpoint accessed, refreshing backend metrics");
        init();
    }
}
