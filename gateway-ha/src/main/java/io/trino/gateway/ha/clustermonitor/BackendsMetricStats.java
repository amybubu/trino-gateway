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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.MBeanExporter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class BackendsMetricStats
{
    private static final Logger log = Logger.get(BackendsMetricStats.class);
    private static final int METRIC_REFRESH_SECONDS = 30;

    private final MBeanExporter exporter;
    private final GatewayBackendManager gatewayBackendManager;
    private Map<String, BackendClusterMetricStats> statsMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    @Inject
    public BackendsMetricStats(GatewayBackendManager gatewayBackendManager, MBeanExporter exporter)
    {
        this.gatewayBackendManager = gatewayBackendManager;
        this.exporter = exporter;
    }

    @PostConstruct
    public void start()
    {
        log.info("Running periodic metric refresh with interval of %d seconds", METRIC_REFRESH_SECONDS);
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                initMetrics();
            }
            catch (Exception e) {
                log.error(e, "Error refreshing backend metrics");
            }
        }, 0, METRIC_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        scheduledExecutor.shutdownNow();
    }

    public void initMetrics()
    {
        // Get current backends from DB
        Set<String> currentBackends = gatewayBackendManager.getAllBackends().stream()
                .map(ProxyBackendConfiguration::getName)
                .collect(Collectors.toSet());

        // Unregister metrics for removed backends
        for (String registeredBackend : statsMap.keySet()) {
            if (!currentBackends.contains(registeredBackend)) {
                try {
                    exporter.unexportWithGeneratedName(BackendClusterMetricStats.class, registeredBackend);
                    log.info("Unregistered metrics for removed cluster: %s", registeredBackend);
                    statsMap.remove(registeredBackend);
                }
                catch (Exception e) {
                    log.error(e, "Failed to unregister metrics for cluster: %s", registeredBackend);
                }
            }
        }

        // Register metrics for added backends
        for (String backend : currentBackends) {
            if (!statsMap.containsKey(backend)) {
                registerBackendMetrics(backend);
            }
        }
    }

    public void registerBackendMetrics(String clusterName)
    {
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
}
