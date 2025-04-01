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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.gateway.ha.clustermonitor.ClusterActivationStats;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.persistence.dao.GatewayBackend;
import io.trino.gateway.ha.persistence.dao.GatewayBackendDao;
import org.jdbi.v3.core.Jdbi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HaGatewayManager
        implements GatewayBackendManager
{
    private static final Logger log = Logger.get(HaGatewayManager.class);

    private final GatewayBackendDao dao;
    private ClusterActivationStats clusterActivationStats;

    public HaGatewayManager(Jdbi jdbi)
    {
        dao = requireNonNull(jdbi, "jdbi is null").onDemand(GatewayBackendDao.class);
    }

    public void setClusterActivationStats(ClusterActivationStats clusterActivationStats)
    {
        System.out.println("INSIDE setClusterActivationStats");
        log.info("AMY LOG setClusterActivationStats clusterActivationStats: %s", clusterActivationStats);
        this.clusterActivationStats = clusterActivationStats;
    }

    @Override
    public List<ProxyBackendConfiguration> getAllBackends()
    {
        List<GatewayBackend> proxyBackendList = dao.findAll();
        return upcast(proxyBackendList);
    }

    @Override
    public List<ProxyBackendConfiguration> getAllActiveBackends()
    {
        List<GatewayBackend> proxyBackendList = dao.findActiveBackend();
        return upcast(proxyBackendList);
    }

    @Override
    public List<ProxyBackendConfiguration> getActiveAdhocBackends()
    {
        try {
            List<GatewayBackend> proxyBackendList = dao.findActiveAdhocBackend();
            return upcast(proxyBackendList);
        }
        catch (Exception e) {
            log.info("Error fetching all backends: %s", e.getLocalizedMessage());
        }
        return ImmutableList.of();
    }

    @Override
    public List<ProxyBackendConfiguration> getActiveBackends(String routingGroup)
    {
        List<GatewayBackend> proxyBackendList = dao.findActiveBackendByRoutingGroup(routingGroup);
        return upcast(proxyBackendList);
    }

    @Override
    public Optional<ProxyBackendConfiguration> getBackendByName(String name)
    {
        List<GatewayBackend> proxyBackendList = dao.findByName(name);
        return upcast(proxyBackendList).stream().findAny();
    }

    @Override
    public void deactivateBackend(String backendName)
    {
        Boolean prevStatus = dao.findFirstByName(backendName).active();
        System.out.println("INSIDE deactivateBackend");
        dao.deactivate(backendName);
        log.info("Backend cluster %s has been deactivated (previous status: active=%s).",
                backendName, prevStatus);
    }

    @Override
    public void activateBackend(String backendName)
    {
        Boolean prevStatus = dao.findFirstByName(backendName).active();
        System.out.println("INSIDE activateBackend");
        dao.activate(backendName);
        log.info("Backend cluster %s has been activated (previous status: active=%s).",
                backendName, prevStatus);
    }

    @Override
    public ProxyBackendConfiguration addBackend(ProxyBackendConfiguration backend)
    {
        System.out.println("AMY LOG: INSIDE addBackend");
        System.out.println("INSIDE addBackend");
        String backendProxyTo = removeTrailingSlash(backend.getProxyTo());
        String backendExternalUrl = removeTrailingSlash(backend.getExternalUrl());
        dao.create(backend.getName(), backend.getRoutingGroup(), backendProxyTo, backendExternalUrl, backend.isActive());
        log.info("AMY ADD clusterActivationStats = %s", clusterActivationStats);
        if (clusterActivationStats != null) {
            clusterActivationStats.initActivationStatusMetricByCluster(backend.getName());
        }
        return backend;
    }

    @Override
    public ProxyBackendConfiguration updateBackend(ProxyBackendConfiguration backend)
    {
        System.out.println("INSIDE updateBackend");
        log.info("AMY LOG: INSIDE updateBackend");
        String backendProxyTo = removeTrailingSlash(backend.getProxyTo());
        String backendExternalUrl = removeTrailingSlash(backend.getExternalUrl());
        GatewayBackend model = dao.findFirstByName(backend.getName());
        if (model == null) {
            dao.create(backend.getName(), backend.getRoutingGroup(), backendProxyTo, backendExternalUrl, backend.isActive());
            log.info("AMY UPDATE model null clusterActivationStats = %s", clusterActivationStats);
            if (clusterActivationStats != null) {
                clusterActivationStats.initActivationStatusMetricByCluster(backend.getName());
            }
        }
        else {
            dao.update(backend.getName(), backend.getRoutingGroup(), backendProxyTo, backendExternalUrl, backend.isActive());
            log.info("AMY UPDATE clusterActivationStats = %s", clusterActivationStats);
            if (clusterActivationStats != null) {
                clusterActivationStats.initActivationStatusMetricByCluster(backend.getName());
            }
        }
        return backend;
    }

    public void deleteBackend(String name)
    {
        dao.deleteByName(name);
    }

    private static List<ProxyBackendConfiguration> upcast(List<GatewayBackend> gatewayBackendList)
    {
        List<ProxyBackendConfiguration> proxyBackendConfigurations = new ArrayList<>();
        for (GatewayBackend model : gatewayBackendList) {
            ProxyBackendConfiguration backendConfig = new ProxyBackendConfiguration();
            backendConfig.setActive(model.active());
            backendConfig.setRoutingGroup(model.routingGroup());
            backendConfig.setProxyTo(model.backendUrl());
            backendConfig.setExternalUrl(model.externalUrl());
            backendConfig.setName(model.name());
            proxyBackendConfigurations.add(backendConfig);
        }
        return proxyBackendConfigurations;
    }

    public static String removeTrailingSlash(String url)
    {
        return url.replaceAll("/$", "");
    }
}
