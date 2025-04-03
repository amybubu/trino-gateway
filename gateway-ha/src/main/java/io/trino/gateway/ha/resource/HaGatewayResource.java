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
import io.trino.gateway.ha.clustermonitor.ClusterActivationStats;
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
    private final GatewayBackendManager haGatewayManager;
    private static final Logger log = Logger.get(HaGatewayResource.class);
    private static ClusterActivationStats clusterActivationStats;

    @Inject
    public HaGatewayResource(GatewayBackendManager haGatewayManager, ClusterActivationStats clusterActivationStats)
    {
        this.haGatewayManager = requireNonNull(haGatewayManager, "haGatewayManager is null");
        this.clusterActivationStats = requireNonNull(clusterActivationStats, "clusterActivationStats is null");
    }

    @Path("/add")
    @POST
    public Response addBackend(ProxyBackendConfiguration backend)
    {
        ProxyBackendConfiguration updatedBackend = haGatewayManager.addBackend(backend);
        log.info("AMY ADD clusterActivationStats = %s", clusterActivationStats);
        if (clusterActivationStats != null) {
            clusterActivationStats.initActivationStatusMetricByCluster(backend.getName());
        }
        return Response.ok(updatedBackend).build();
    }

    @Path("/update")
    @POST
    public Response updateBackend(ProxyBackendConfiguration backend)
    {
        ProxyBackendConfiguration updatedBackend = haGatewayManager.updateBackend(backend);
        log.info("AMY UPDATE clusterActivationStats = %s", clusterActivationStats);
        if (clusterActivationStats != null) {
            clusterActivationStats.initActivationStatusMetricByCluster(backend.getName());
        }
        return Response.ok(updatedBackend).build();
    }

    @Path("/delete")
    @POST
    public Response removeBackend(String name)
    {
        ((HaGatewayManager) haGatewayManager).deleteBackend(name);
        return Response.ok().build();
    }
}
