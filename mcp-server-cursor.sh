#!/bin/bash
# Wrapper script for Cursor MCP - assumes Vault authentication is already done

export VAULT_ADDR=https://vault.us1.staging.dog

# Check if we have a valid vault token
if ! vault token lookup &>/dev/null; then
    echo "Error: No valid Vault token found. Please run:" >&2
    echo "  vault login -method=oidc" >&2
    exit 1
fi

export VAULT_TOKEN=$(vault print token)

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the MCP server with aws-vault from the project directory
cd "$SCRIPT_DIR" && aws-vault exec sso-staging-engineering -- uv run -m spark_history_mcp.core.main

