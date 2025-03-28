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
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import org.weakref.jmx.Managed;

import java.util.List;

public class ClusterActivationStats
{
    public static final String ACTIVATION_STATUS_METRIC_SUFFIX = ".activationStatus";
    private final GatewayBackendManager gatewayBackendManager;

    @Inject
    public ClusterActivationStats(GatewayBackendManager gatewayBackendManager)
    {
        this.gatewayBackendManager = gatewayBackendManager;
    }

    public void initActivationStatusMetrics()
    {
        List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();
        for (ProxyBackendConfiguration backend : backends) {
            String clusterId = backend.getName();
            createActivationStatusMetric(clusterId);
        }
    }

    public void initActivationStatusMetricByCluster(String clusterName)
    {
        createActivationStatusMetric(clusterName);
    }

    @Managed
    public int createActivationStatusMetric(String clusterName)
    {
        return gatewayBackendManager.getBackendByName(clusterName)
                .map(backend -> backend.isActive() ? 1 : 0)
                .orElse(-1);
    }
}
