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
package io.trino.gateway.ha.router;

import io.trino.gateway.ha.clustermonitor.ClusterActivationStats;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.persistence.JdbcConnectionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import static io.trino.gateway.ha.TestingJdbcConnectionManager.createTestingJdbcConnectionManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@TestInstance(Lifecycle.PER_CLASS)
final class TestHaGatewayManager
{
    private HaGatewayManager haGatewayManager;

    @BeforeAll
    void setUp()
    {
        JdbcConnectionManager connectionManager = createTestingJdbcConnectionManager();
        haGatewayManager = new HaGatewayManager(connectionManager.getJdbi());
        //ClusterActivationStats mockStats = mock(ClusterActivationStats.class);
        //haGatewayManager.setClusterActivationStats(mockStats);
    }

    @Test
    void testGatewayManager()
    {
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setActive(true);
        backend.setRoutingGroup("adhoc");
        backend.setName("adhoc1");
        backend.setProxyTo("adhoc1.trino.gateway.io");
        backend.setExternalUrl("adhoc1.trino.gateway.io");
        ProxyBackendConfiguration updated = haGatewayManager.addBackend(backend);
        assertThat(updated).isEqualTo(backend);

        // Get backends
        assertThat(haGatewayManager.getAllBackends()).hasSize(1);
        assertThat(haGatewayManager.getActiveBackends("adhoc")).hasSize(1);
        assertThat(haGatewayManager.getActiveBackends("unknown")).isEmpty();
        assertThat(haGatewayManager.getActiveAdhocBackends()).hasSize(1);

        // Update a backend
        ProxyBackendConfiguration adhoc = new ProxyBackendConfiguration();
        adhoc.setActive(false);
        adhoc.setRoutingGroup("adhoc");
        adhoc.setName("new-adhoc1");
        adhoc.setProxyTo("adhoc1.trino.gateway.io");
        adhoc.setExternalUrl("adhoc1.trino.gateway.io");
        haGatewayManager.updateBackend(adhoc);
        assertThat(haGatewayManager.getActiveBackends("adhoc")).hasSize(1);
        assertThat(haGatewayManager.getAllBackends())
                .extracting(ProxyBackendConfiguration::getRoutingGroup)
                .containsExactly("adhoc", "adhoc");

        adhoc.setActive(false);
        adhoc.setRoutingGroup("etl");
        adhoc.setName("adhoc1");
        adhoc.setProxyTo("adhoc2.trino.gateway.io");
        adhoc.setExternalUrl("adhoc2.trino.gateway.io");
        haGatewayManager.updateBackend(adhoc);
        assertThat(haGatewayManager.getActiveBackends("adhoc")).isEmpty();
        assertThat(haGatewayManager.getAllBackends())
                .extracting(ProxyBackendConfiguration::getRoutingGroup)
                .containsExactly("adhoc", "etl");

        // Delete a backend
        haGatewayManager.deleteBackend("adhoc1");
        assertThat(haGatewayManager.getAllBackends())
                .extracting(ProxyBackendConfiguration::getRoutingGroup)
                .containsExactly("adhoc");
    }

    @Test
    void testRemoveTrailingSlashInUrl()
    {
        ProxyBackendConfiguration etl = new ProxyBackendConfiguration();
        etl.setActive(false);
        etl.setRoutingGroup("etl");
        etl.setName("new-etl1");
        etl.setProxyTo("https://etl1.trino.gateway.io:443/");
        etl.setExternalUrl("https://etl1.trino.gateway.io:443/");
        haGatewayManager.addBackend(etl);

        assertThat(haGatewayManager.getBackendByName("new-etl1").map(ProxyBackendConfiguration::getProxyTo).orElseThrow()).isEqualTo("https://etl1.trino.gateway.io:443");
        assertThat(haGatewayManager.getBackendByName("new-etl1").map(ProxyBackendConfiguration::getExternalUrl).orElseThrow()).isEqualTo("https://etl1.trino.gateway.io:443");

        ProxyBackendConfiguration etl2 = new ProxyBackendConfiguration();
        etl2.setActive(false);
        etl2.setRoutingGroup("etl2");
        etl2.setName("new-etl1");
        etl2.setProxyTo("https://etl2.trino.gateway.io:443/");
        etl2.setExternalUrl("https://etl2.trino.gateway.io:443/");
        haGatewayManager.updateBackend(etl2);

        assertThat(haGatewayManager.getBackendByName("new-etl1").map(ProxyBackendConfiguration::getProxyTo).orElseThrow()).isEqualTo("https://etl2.trino.gateway.io:443");
        assertThat(haGatewayManager.getBackendByName("new-etl1").map(ProxyBackendConfiguration::getExternalUrl).orElseThrow()).isEqualTo("https://etl2.trino.gateway.io:443");
    }
}
