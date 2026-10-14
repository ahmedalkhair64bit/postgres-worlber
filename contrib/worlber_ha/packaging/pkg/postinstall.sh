#!/bin/sh
# Create the config directory; never start the service by itself.
mkdir -p /etc/worlber_ha /var/lib/worlber_ha
if id postgres >/dev/null 2>&1; then
    chown postgres:postgres /etc/worlber_ha /var/lib/worlber_ha
    chmod 0750 /etc/worlber_ha
    chmod 0700 /var/lib/worlber_ha
fi
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
echo "worlber-ha-agent installed. Join a cluster with:"
echo "  worlber-ha-agent init --join-file <token file>   (token from SELECT worlber_ha.join_token())"
echo "  systemctl enable --now worlber-ha-agent"
exit 0
