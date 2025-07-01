# Grafana Cloud Integration Setup

This guide explains how to configure the orchestrator to send metrics to Grafana Cloud instead of running a local Grafana instance.

## Overview

The orchestrator now supports two monitoring modes:

1. **Local Monitoring** (default): Runs Prometheus and Grafana on a dedicated monitoring instance
2. **Grafana Cloud**: Uses Grafana Alloy to forward metrics to Grafana Cloud's Mimir service

## Prerequisites

Before setting up Grafana Cloud integration, you'll need:

1. A Grafana Cloud account
2. Access to your organization's Grafana Cloud Mimir endpoint
3. Valid credentials (username and password) for pushing metrics

## Configuration

### 1. Update your settings file

Add the following configuration to your `settings.yml` file:

```yaml
# Enable Grafana Cloud mode
use_grafana_cloud: true

# Grafana Cloud Mimir endpoint (get this from your PE team)
grafana_cloud_url: "https://prometheus-prod-01-eu-west-0.grafana.net/api/prom/push"

# Grafana Cloud credentials
grafana_cloud_username: "your-username"
grafana_cloud_password_file: "/path/to/password/file"
```

### 2. Create a password file

Store your Grafana Cloud password in a secure file:

```bash
echo "your-grafana-cloud-password" > /path/to/password/file
chmod 600 /path/to/password/file
```

### 3. Required Information from PE Team

You'll need to obtain the following from your Platform Engineering team:

- **Mimir endpoint URL**: The complete URL for pushing metrics (e.g., `https://prometheus-prod-01-eu-west-0.grafana.net/api/prom/push`)
- **Username**: Your Grafana Cloud username for metric ingestion
- **Password**: The corresponding password (store this securely in a file)

## Configuration Options

### Required Fields (when `use_grafana_cloud: true`)

- `grafana_cloud_url`: The Mimir endpoint URL for your organization
- `grafana_cloud_username`: Username for authentication
- `grafana_cloud_password_file`: Path to file containing the password

### Automatic Labels

The orchestrator automatically adds the following labels to all metrics:
- `host`: Hostname of the monitoring instance
- `system`: Always set to "tidehunter"
- `commit`: The git commit being benchmarked

## Example Configuration

```yaml
---
testbed_id: "myuser-tidehunter"
cloud_provider: aws
token_file: "/Users/myuser/.aws/credentials"
ssh_private_key_file: "/Users/myuser/.ssh/aws"
regions:
  - us-west-1
specs: m5d.8xlarge
repository:
  url: https://github.com/MystenLabs/tidehunter.git
  commit: main

# Monitoring configuration
monitoring: true
use_grafana_cloud: true

# Grafana Cloud settings
grafana_cloud_url: "https://prometheus-prod-01-eu-west-0.grafana.net/api/prom/push"
grafana_cloud_username: "incoming_metrics"
grafana_cloud_password_file: "/home/myuser/.grafana_cloud_password"
```

## How It Works

When `use_grafana_cloud: true` is set:

1. **Alloy Installation**: The orchestrator installs Grafana Alloy instead of Prometheus/Grafana
2. **Configuration Generation**: Alloy is configured to scrape metrics from benchmark nodes
3. **Metric Forwarding**: Alloy forwards all metrics to your Grafana Cloud Mimir endpoint
4. **Label Enhancement**: All metrics are enhanced with the configured labels
5. **Local Metrics Collection**: The orchestrator disables local metrics collection since all metrics are forwarded directly to Grafana Cloud

### Generated Alloy Configuration

The orchestrator automatically generates an Alloy configuration similar to:

```hcl
prometheus.scrape "tidehunter_benchmark" {
    targets = [
        {__address__ = "10.0.1.100:9092"},
        {__address__ = "10.0.1.101:9092"},
    ]
    forward_to = [prometheus.remote_write.grafana_cloud.receiver]
    job_name   = "tidehunter_benchmark"
}

prometheus.remote_write "grafana_cloud" {
    external_labels = {
        host    = "monitoring-instance"
        system  = "tidehunter"
        commit  = "main"
    }

    endpoint {
        name = "grafana-cloud"
        url  = "https://prometheus-prod-01-eu-west-0.grafana.net/api/prom/push"
        
        basic_auth {
            username = "incoming_metrics"
            password = "your-password"
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **"could not perform the initial load successfully"**: This indicates a configuration syntax error
   - Run `sudo alloy validate /etc/alloy/config.alloy` to check syntax
   - Check if password file contains special characters that need escaping
   - Ensure all required fields (username, password, URL) are properly set

2. **Authentication Errors**: Verify your username and password file
   - Check that the password file exists and is readable
   - Verify credentials with your PE team

3. **Network Connectivity**: Ensure the monitoring instance can reach the Grafana Cloud endpoint
   - Test with: `curl -v "https://your-endpoint.com/api/prom/push"`

4. **Missing Metrics**: Check Alloy logs: `sudo journalctl -u alloy -f`
   - Look for scraping errors or authentication failures
   - Verify benchmark nodes are running and exposing metrics

5. **Service Restart Loops**: If Alloy keeps restarting
   - Check configuration syntax with `sudo alloy validate /etc/alloy/config.alloy`
   - Look for empty credentials or malformed URLs

### Validation

To verify the setup is working:

1. Check Alloy status: `sudo systemctl status alloy`
2. View Alloy logs: `sudo journalctl -u alloy -f`
3. Validate the configuration: `sudo alloy validate /etc/alloy/config.alloy`
4. Check the generated configuration: `sudo cat /etc/alloy/config.alloy`
5. Check your Grafana Cloud dashboard for incoming metrics

### Configuration Debugging

If Alloy fails to start, check the following:

1. **Configuration Syntax**: Validate the configuration file
   ```bash
   sudo alloy validate /etc/alloy/config.alloy
   ```

2. **View Generated Config**: Check what was actually generated
   ```bash
   sudo cat /etc/alloy/config.alloy
   ```

3. **Check Credentials**: Ensure your password file is readable and contains valid credentials
   ```bash
   cat /path/to/password/file  # Should show your password
   ```

4. **Network Connectivity**: Test connection to Grafana Cloud
   ```bash
   curl -v "https://your-grafana-cloud-endpoint.com/api/prom/push"
   ```

### Switching Back to Local Mode

To switch back to local Prometheus/Grafana:

```yaml
use_grafana_cloud: false
```

The orchestrator will automatically install and configure local Prometheus and Grafana instances.

## Security Considerations

- Store passwords in secure files with restricted permissions (600)
- Use environment variables for sensitive configuration when possible
- Regularly rotate Grafana Cloud credentials
- Ensure monitoring instances have minimal network exposure

## Support

If you encounter issues:

1. Check the orchestrator logs for configuration errors
2. Verify your Grafana Cloud credentials with your PE team
3. Test connectivity to the Mimir endpoint manually
4. Review Alloy configuration in `/etc/alloy/config.alloy` 