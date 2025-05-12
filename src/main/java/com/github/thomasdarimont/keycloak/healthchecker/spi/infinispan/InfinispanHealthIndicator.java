package com.github.thomasdarimont.keycloak.healthchecker.spi.infinispan;

import com.github.thomasdarimont.keycloak.healthchecker.model.HealthStatus;
import com.github.thomasdarimont.keycloak.healthchecker.model.KeycloakHealthStatus;
import com.github.thomasdarimont.keycloak.healthchecker.spi.AbstractHealthIndicator;
import io.quarkus.arc.Arc;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.manager.EmbeddedCacheManager;
import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.quarkus.runtime.storage.infinispan.CacheManagerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InfinispanHealthIndicator extends AbstractHealthIndicator {

    private static final String KEYCLOAK_CACHE_MANAGER_JNDI_NAME = "java:jboss/infinispan/container/keycloak";

    private KeycloakSession session;
    protected final String jndiName;

    public InfinispanHealthIndicator(KeycloakSession session, Config.Scope config) {
        super("infinispan");
        this.session = session;
        this.jndiName = config.get("jndiName", KEYCLOAK_CACHE_MANAGER_JNDI_NAME);
    }

    @Override
    public HealthStatus check() {
        Health infinispanHealth = getInfinispanHealth();
        ClusterHealth clusterHealth = infinispanHealth.getClusterHealth();

        KeycloakHealthStatus status = determineClusterHealth(clusterHealth);

        List<Map<Object, Object>> detailedCacheHealthInfo = infinispanHealth.getCacheHealth().stream().map(c -> {
            Map<Object, Object> item = new LinkedHashMap<>();
            item.put("cacheName", c.getCacheName());
            item.put("healthStatus", c.getStatus());
            return item;
        }).toList();

        status//
                .withAttribute("hostInfo", infinispanHealth.getHostInfo())
                .withAttribute("clusterName", clusterHealth.getClusterName()) //
                .withAttribute("healthStatus", clusterHealth.getHealthStatus()) //
                .withAttribute("numberOfNodes", clusterHealth.getNumberOfNodes()) //
                .withAttribute("nodeNames", clusterHealth.getNodeNames())
                .withAttribute("cacheDetails", detailedCacheHealthInfo)
        ;

        return status;
    }

    private Health getInfinispanHealth() {
        return lookupCacheManager().getHealth();
    }

    private EmbeddedCacheManager lookupCacheManager() {
        // Manual lookup via Arc for Keycloak.X
        return Arc.container().instance(CacheManagerFactory.class).get().getOrCreateEmbeddedCacheManager(this.session);
    }

    private KeycloakHealthStatus determineClusterHealth(ClusterHealth clusterHealth) {
        switch (clusterHealth.getHealthStatus()) {
            case HEALTHY:
                return reportUp();
            case HEALTHY_REBALANCING:
                return reportUp();
            case DEGRADED, FAILED:
                return reportDown();
            default:
                return reportDown();
        }
    }
}
