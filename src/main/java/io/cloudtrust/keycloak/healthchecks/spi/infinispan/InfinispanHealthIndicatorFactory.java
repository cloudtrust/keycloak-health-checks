package io.cloudtrust.keycloak.healthchecks.spi.infinispan;

import io.cloudtrust.keycloak.healthchecks.spi.HealthIndicator;
import io.cloudtrust.keycloak.healthchecks.spi.HealthIndicatorFactory;
import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class InfinispanHealthIndicatorFactory implements HealthIndicatorFactory {

    private static final String ID = "infinispan-health";

    private Config.Scope config;

    @Override
    public HealthIndicator create(KeycloakSession session) {
        return new InfinispanHealthIndicator(session, config);
    }

    @Override
    public void init(Config.Scope config) {
        this.config = config;
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        //NOOP
    }

    @Override
    public void close() {
        //NOOP
    }

    @Override
    public String getId() {
        return ID;
    }
}