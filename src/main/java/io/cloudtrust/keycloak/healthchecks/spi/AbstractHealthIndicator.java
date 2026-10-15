package io.cloudtrust.keycloak.healthchecks.spi;

import io.cloudtrust.keycloak.healthchecks.model.KeycloakHealthStatus;

public abstract class AbstractHealthIndicator implements HealthIndicator {

    private final String name;

    public AbstractHealthIndicator(String name) {
        this.name = name;
    }

    protected KeycloakHealthStatus reportUp() {
        return KeycloakHealthStatus.up(getName());
    }

    protected KeycloakHealthStatus reportDown() {
        return KeycloakHealthStatus.down(getName());
    }

    @Override
    public void close() {
        //NOOP
    }

    public String getName() {
        return name;
    }
}
