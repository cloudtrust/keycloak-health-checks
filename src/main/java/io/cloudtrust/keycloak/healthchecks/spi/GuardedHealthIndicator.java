package io.cloudtrust.keycloak.healthchecks.spi;


import io.cloudtrust.keycloak.healthchecks.model.HealthStatus;
import io.cloudtrust.keycloak.healthchecks.model.KeycloakHealthStatus;

public class GuardedHealthIndicator implements HealthIndicator {

    private final HealthIndicator healthIndicator;

    public GuardedHealthIndicator(HealthIndicator healthIndicator) {
        this.healthIndicator = healthIndicator;
    }

    @Override
    public String getName() {
        return this.healthIndicator.getName();
    }

    @Override
    public HealthStatus check() {

        try {
            return this.healthIndicator.check();
        } catch (Exception ex) {
            return KeycloakHealthStatus.down(this.healthIndicator.getName()) //
                    .withAttribute("error", "health-check")
                    .withAttribute("errorMessage", ex.getMessage());
        }
    }

    @Override
    public boolean isApplicable() {
        return healthIndicator.isApplicable();
    }
}
