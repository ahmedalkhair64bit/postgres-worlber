#!/bin/bash
# Example --on-role-change hook for Google Cloud: move an alias IP range to
# the VM that becomes primary.  ARP-based VIPs do not work in a GCP VPC; this
# (or an internal TCP load balancer with health check
# http://<node>:7400/health/primary) is the cloud equivalent.
#
# Called by the agent as:  role-change-gcp-alias-ip.sh <role> <cluster> <node>
# Needs: gcloud, a service account with compute.instances.updateNetworkInterface
# Configure:
ALIAS_IP=${ALIAS_IP:-10.128.0.100/32}
ZONE=${ZONE:-$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')}
INSTANCE=${INSTANCE:-$(hostname -s)}

role=$1
set -e
case "$role" in
  primary)
    # gcloud moves the alias: other instances must not hold it anymore; the
    # agent of the old primary removes it when it is demoted or fenced.
    gcloud compute instances network-interfaces update "$INSTANCE" --zone "$ZONE" \
        --aliases "$ALIAS_IP"
    echo "alias $ALIAS_IP attached to $INSTANCE"
    ;;
  replica|stopped)
    gcloud compute instances network-interfaces update "$INSTANCE" --zone "$ZONE" \
        --aliases "" || true
    echo "alias removed from $INSTANCE"
    ;;
esac
