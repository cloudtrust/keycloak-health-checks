package com.github.thomasdarimont.keycloak.healthchecker.spi;

import com.github.thomasdarimont.keycloak.healthchecker.model.KeycloakHealthStatus;

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
