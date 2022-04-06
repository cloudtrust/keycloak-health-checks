# Keycloak Health Checks

A collection of health-checks for Keycloak subsystems.

## Supported Checks

1. Filesystem (Instance Level)
1. Database (Instance Level)
1. Infinispan Cluster state (Instance Level)

## Requirements

* Keycloak 17.0.1

## Build

`mvn package`

## Installation

Use the `install.sh` script available in the tar.gz package:
`./install.sh /path/to/keycloak/home`   

## Uninstall

Use the `install.sh` script available in the tar.gz package:
`./install.sh /path/to/keycloak/home -u`   

## Configuration

The following health-check providers are supported: 
- `infinispan`
- `database`
- `ldap`
- `filesystem`

To disable the `filesystem-health` check, one can use the following config setting in keycloak.conf
```
spi-health-filesystem-health-enabled=false
```

## Running example

Start Keycloak and browse to: `http://localhost:8080/auth/realms/master/health/check`

You should now see something like with `HTTP Status 200 OK`

```
curl -v http://localhost:8080/auth/realms/master/health/check | jq -C .
...
< HTTP/1.1 200 OK
< Connection: keep-alive
< Content-Type: application/json
< Content-Length: 1090
< Date: Wed, 06 Feb 2019 19:09:42 GMT
```

```json
{
  "details": {
    "database": {
      "connection": "established",
      "state": "UP"
    },
    "filesystem": {
      "freebytes": 288779120640,
      "state": "UP"
    },
    "infinispan": {
      "numberOfNodes": 1,
      "state": "UP",
      "healthStatus": "HEALTHY",
      "nodeNames": [],
      "cacheDetails": [
        {
          "cacheName": "realms",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "authenticationSessions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "sessions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "authorizationRevisions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "clientSessions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "work",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "keys",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "users",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "loginFailures",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "offlineClientSessions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "authorization",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "realmRevisions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "offlineSessions",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "actionTokens",
          "healthStatus": "HEALTHY"
        },
        {
          "cacheName": "userRevisions",
          "healthStatus": "HEALTHY"
        }
      ],
      "clusterName": "ISPN"
    }
  },
  "name": "keycloak",
  "state": "UP"
}
```

In case a check fails, you should get a response with `HTTP Status 503 SERVICE UNAVAILABLE` with a body like:
```json
{
   "details":{
      "filesystem":{
         "state":"UP"
      },
      "database":{
         "message":"javax.resource.ResourceException: IJ000453: Unable to get managed connection for java:jboss/datasources/KeycloakDS",
         "state":"DOWN"
      },
      "infinispan": {
         "numberOfNodes": 1,
         "state": "UP",
         "healthStatus": "HEALTHY",
         "nodeNames": [],
         "cacheDetails": [
         {
           "cacheName": "realms",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "authenticationSessions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "sessions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "authorizationRevisions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "clientSessions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "work",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "keys",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "users",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "loginFailures",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "offlineClientSessions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "authorization",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "realmRevisions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "offlineSessions",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "actionTokens",
           "healthStatus": "HEALTHY"
         },
         {
           "cacheName": "userRevisions",
           "healthStatus": "HEALTHY"
         }],
         "clusterName": "ISPN"
        }
   },
   "name":"keycloak",
   "state":"DOWN"
}
```

You can also query the health-checks individually by appending the name of the check to the end of `/health` endpoint URL. 

The following health-checks are currently available:
* `database`
* `filesystem` (currently disabled)
* `infinispan` 

```
$ curl -s http://localhost:8080/auth/realms/master/health/check/database | jq -C .
{
  "state": "UP",
  "details": {
    "connection": "established",
    "state": "UP"
  },
  "name": "database"
}

```

## Securing the health endpoint

The health endpoint is only available on the _master_ realm. It should not be directly exposed to the internet. There are multiple ways to properly secure Keycloak endpoints
like firewalls, reverse-proxies, or JBoss / wildfly specific configuration options.

The keycloak documentation provides additional information about [securing admin endpoints](https://github.com/keycloak/keycloak-documentation/blob/master/server_admin/topics/threat/admin.adoc#port-restriction). The same mechanism
can be used to protect the health-endpoints. 