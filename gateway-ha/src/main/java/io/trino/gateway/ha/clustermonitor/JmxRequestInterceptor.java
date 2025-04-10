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
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;

@Provider
public class JmxRequestInterceptor
        implements ContainerRequestFilter
{
    private final BackendsMetricStats backendsMetricStats;

    @Inject
    public JmxRequestInterceptor(BackendsMetricStats backendsMetricStats)
    {
        this.backendsMetricStats = backendsMetricStats;
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
            throws IOException
    {
        String path = requestContext.getUriInfo().getPath();
        if (path.contains("v1/jmx")) {
            backendsMetricStats.refreshOnJmxAccess();
        }
    }
}
