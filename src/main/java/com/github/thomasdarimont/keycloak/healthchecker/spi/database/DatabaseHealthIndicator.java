package com.github.thomasdarimont.keycloak.healthchecker.spi.database;

import com.github.thomasdarimont.keycloak.healthchecker.model.HealthStatus;
import com.github.thomasdarimont.keycloak.healthchecker.spi.AbstractHealthIndicator;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.utils.StringUtil;

import javax.enterprise.inject.spi.CDI;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

public class DatabaseHealthIndicator extends AbstractHealthIndicator {
    private static final Logger log = Logger.getLogger(DatabaseHealthIndicator.class);
	public static final String KEYCLOAK_DATASOURCE_JNDI_NAME = "java:jboss/datasources/KeycloakDS";

	protected final String healthQuery;

	protected final String jndiName;

	protected final int connectionTimeoutMillis;

	public DatabaseHealthIndicator(Config.Scope config) {
        super("database");
        this.jndiName = config.get("jndiName", KEYCLOAK_DATASOURCE_JNDI_NAME);
        this.healthQuery = config.get("query", "SELECT 1");
        this.connectionTimeoutMillis = config.getInt("connectionTimeout", 1000);
    }

    @Override
    public HealthStatus check() {
        try {
        	DataSource dataSource = lookupDataSource();
        	if (isDatabaseReady(dataSource, healthQuery)) {
                return reportUp()
                        .withAttribute("connection", "established");
            }
        } catch (Exception ex) {
            return reportDown()
                    .withAttribute("connection", "error")
                    .withAttribute("message", ex.getMessage());
        }

        return reportDown();
    }

    protected DataSource lookupDataSource() throws Exception {
    	return CDI.current().select(DataSource.class).get();
    }

    protected boolean isDatabaseReady(DataSource dataSource, String healthQuery) throws Exception {
    	try (Connection connection = dataSource.getConnection()) {
            boolean valid;
            if (StringUtil.isNotBlank(healthQuery)) {
            	try (Statement statement = connection.createStatement()) {
            		try {
                        valid = statement.execute(healthQuery);
                    } catch (SQLSyntaxErrorException syntaxErrorException) {
                        log.errorf("Health Query is invalid", syntaxErrorException.getMessage());
                        valid = false;
                    }
            	}
        	} else {
                valid = connection.isValid(connectionTimeoutMillis);
            }
            log.debugf("Connection is Valid %s", valid);
            return valid;
    	}
    }
}
