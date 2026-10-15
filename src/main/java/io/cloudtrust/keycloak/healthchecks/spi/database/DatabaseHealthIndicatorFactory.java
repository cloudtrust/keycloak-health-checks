package io.cloudtrust.keycloak.healthchecks.spi.database;

import io.cloudtrust.keycloak.healthchecks.spi.HealthIndicator;
import io.cloudtrust.keycloak.healthchecks.spi.HealthIndicatorFactory;
import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class DatabaseHealthIndicatorFactory implements HealthIndicatorFactory {

    public static final String ID = "database-health";

    private Config.Scope config;

    @Override
    public HealthIndicator create(KeycloakSession session) {
        return new DatabaseHealthIndicator(config);
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
