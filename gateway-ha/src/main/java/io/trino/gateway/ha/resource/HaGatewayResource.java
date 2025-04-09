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
package io.trino.gateway.ha.resource;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.gateway.ha.clustermonitor.BackendsMetricStats;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.HaGatewayManager;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

@RolesAllowed("API")
@Path("gateway/backend/modify")
@Produces(MediaType.APPLICATION_JSON)
public class HaGatewayResource
{
    private static final Logger log = Logger.get(HaGatewayResource.class);
    private final GatewayBackendManager haGatewayManager;
    private BackendsMetricStats backendsMetricStats;

    @Inject
    public HaGatewayResource(GatewayBackendManager haGatewayManager, BackendsMetricStats backendsMetricStats)
    {
        this.haGatewayManager = requireNonNull(haGatewayManager, "haGatewayManager is null");
        this.backendsMetricStats = requireNonNull(backendsMetricStats, "backendsMetricStats is null");
    }

    @Path("/add")
    @POST
    public Response addBackend(ProxyBackendConfiguration backend)
    {
        ProxyBackendConfiguration updatedBackend = haGatewayManager.addBackend(backend);
        backendsMetricStats.registerBackendMetrics(backend);
        log.info("Added backend %s and registered its metrics", backend.getName());
        return Response.ok(updatedBackend).build();
    }

    @Path("/update")
    @POST
    public Response updateBackend(ProxyBackendConfiguration backend)
    {
        boolean backendExists = haGatewayManager.getBackendByName(backend.getName()).isPresent();
        ProxyBackendConfiguration updatedBackend = haGatewayManager.updateBackend(backend);
        if (!backendExists) {
            backendsMetricStats.registerBackendMetrics(updatedBackend);
            log.info("Registered metrics for new backend %s created via update", backend.getName());
        }
        return Response.ok(updatedBackend).build();
    }

    @Path("/delete")
    @POST
    public Response removeBackend(String name)
    {
        backendsMetricStats.unregisterBackendMetrics(name);
        ((HaGatewayManager) haGatewayManager).deleteBackend(name);
        log.info("Removed backend %s and unregistered its metrics", name);
        return Response.ok().build();
    }
}
