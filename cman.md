# Oracle CMAN Operations Checklist for Oracle RAC Behind a Firewall for Confluent XStream CDC

This is a concise operator-facing guidance for customer DBA teams. It assumes CMAN is already deployed, the RAC database is registering services through `remote_listener` to both SCAN and CMAN, and the XStream CDC connector uses the CMAN hostname and port as its database entry point.

## Purpose

Use Oracle Connection Manager as the Oracle-aware proxy in front of RAC when the connector cannot directly reach RAC nodes. CMAN is designed for database transparency, proxying, and HA-aware Oracle connectivity, whereas a generic TCP proxy can fail with RAC because SCAN may redirect the client to a backend node that is not reachable from the client network, causing connection timeout errors like `ORA-12170`.

## Steady-state design assumptions

The operating model should look like this:

- RAC instances register services to both SCAN and CMAN through `remote_listener`.
- CMAN listens on a client-reachable endpoint across the firewall.
- The connector connects to CMAN, and CMAN hides RAC connection complexity from the client.

For RAC listener security, use Valid Node Checking for Registration (VNCR) so only approved RAC nodes can register services. Oracle documents VNCR as the control that prevents unintended or malicious listener registration.

## 1. Start / restart procedure

### Start CMAN

```bash
cmctl startup
cmctl show services
```

CMAN is considered up when the listener starts successfully and `show services` displays the expected database services.

### Restart CMAN after config changes

Use this after changes to `cman.ora`, DNS, firewall rules, or RAC listener registration:

```bash
cmctl shutdown
cmctl startup
cmctl show services
```

Oracle’s basic validation pattern is to start CMAN with `cmctl` and then test listener reachability with SQL*Plus.

### Stop CMAN

Stop only during an approved maintenance window, because all connector sessions using the CMAN endpoint will be impacted.

## 2. Health checks

Run these checks after startup, after RAC maintenance, and whenever the connector reports connection issues.

### Check 1: CMAN process is listening

From a client or the CMAN host:

```bash
sqlplus dummy/dummy@cman-host:1521/nonexistent_service
```

If CMAN is listening, Oracle’s documented expected result is `ORA-12514` (`listener does not currently know of service requested`). If CMAN is not listening, expect `ORA-12541` (`no listener`).

### Check 2: RAC services are registered into CMAN

```bash
cmctl show services
```

Confirm the XStream-related service is visible.

### Check 3: Registration path is intact

On the RAC side, confirm `remote_listener` still includes both SCAN and CMAN aliases. If RAC services disappear from CMAN after DB or GI changes, re-check `remote_listener` and re-register services.

### Check 4: VNCR is still aligned

Confirm invited nodes still match all current RAC node IPs for SCAN/listener registration. If node IPs changed and VNCR was not updated, service registration can be rejected.

## 3. Routine operating checks

### Daily

- Verify `cmctl show services` returns the expected service set.
- Verify the connector can still resolve and reach the CMAN host/port.
- Review listener logs for repeated registration failures or connection spikes.

### After any RAC patching or node maintenance

- Reconfirm RAC services re-registered through CMAN.
- Run a SQL*Plus connectivity test through CMAN.
- Validate the connector reconnects cleanly.

### After firewall or DNS changes

- Validate the CMAN endpoint is still reachable from the connector network.
- Re-test service visibility with `cmctl show services`.
- Re-run end-to-end connector connectivity.

## 4. Change management guidance

### When changing `cman.ora`

A minimal CMAN config needs an instance name, listening endpoint, and access rule. Oracle’s starter config commonly uses an allow-all rule for initial bring-up, but that is just a starting point and should be tightened for production.

Operational rule:

- Validate with broad rules first if needed during setup.
- For production, restrict source, destination, and service patterns as tightly as possible.

### When changing RAC node membership or IPs

Update VNCR invited-node lists on the RAC listeners so registration from valid nodes continues to work.

### When changing service names

After service changes:

1. restart or reload the relevant listener path,
2. confirm service registration in CMAN,
3. validate the connector uses the correct service name.

## 5. Failover expectations

RAC normally uses SCAN listeners to direct clients to the least-loaded or correct instance. A simple TCP proxy breaks this model because the redirect target may be unreachable from the client side of the firewall. CMAN is the preferred Oracle-aware proxy because it preserves Oracle connection handling rather than just forwarding raw TCP.

Operationally, after an instance failover or service relocation:

- confirm the service reappears in `cmctl show services`,
- test a new session through CMAN,
- then validate connector recovery.

## 6. Troubleshooting quick map

### Symptom: `ORA-12541: TNS:no listener`

Likely causes:

- CMAN is down
- wrong port
- firewall closed

Check:

```bash
cmctl startup
cmctl show services
```

Oracle uses `ORA-12541` as the expected symptom when CMAN is not listening.

### Symptom: `ORA-12514`

Meaning:

- CMAN is reachable, but the requested service is not registered or service name is wrong.

Actions:

- verify service name,
- verify RAC registration to CMAN,
- verify `remote_listener`,
- re-check `cmctl show services`.

### Symptom: `ORA-12170: TNS connect timeout`

Most likely in this topology:

- client reached the initial listener but was redirected toward an unreachable RAC node,
- or a non-Oracle proxy is in the path.

This is the classic failure mode of using a generic proxy in front of RAC.

Actions:

- confirm the connector is pointing to CMAN, not a generic TCP proxy,
- confirm CMAN is the only client-facing entry point,
- confirm firewall paths from CMAN to RAC nodes are open.

### Symptom: services missing from `cmctl show services`

Likely causes:

- `remote_listener` no longer includes CMAN,
- VNCR rejected registration,
- RAC listener restarted and services did not re-register yet.

Actions:

- re-check `remote_listener`,
- review VNCR settings and invited-node lists,
- force service re-registration from RAC if needed.

## 7. Operator do / don’t list

### Do

- Use CMAN as the client-facing Oracle proxy for RAC behind the firewall.
- Keep VNCR enabled on RAC listeners and maintain the invited node list.
- Validate with SQL*Plus and `cmctl show services` after every network or RAC change.

### Don’t

- Don’t use a generic reverse proxy such as Nginx for production RAC proxying if the client cannot reach backend RAC nodes; it loses proper RAC behavior and can break on redirects.
- Don’t leave “allow all” CMAN rules in place longer than needed for initial validation.

## 8. Minimal operations command set

```bash
# Start CMAN
cmctl startup

# Stop CMAN
cmctl shutdown

# View registered services
cmctl show services

# Listener reachability test
sqlplus dummy/dummy@cman-host:1521/nonexistent_service
```
