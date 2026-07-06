"""Shared helpers for cartography ingestion benchmarks (grounded on real code paths)."""

import random
import string
import time

import neo4j

NEO4J_URI = "bolt://localhost:7687"
ACCOUNT_ID = "123456789012"


def get_session():
    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=None)
    return driver.session()


def wipe(session):
    # Delete everything in batches, then drop all indexes for a clean slate.
    session.run(
        "MATCH ()-[r]->() CALL { WITH r DELETE r } IN TRANSACTIONS OF 10000 ROWS"
    ).consume()
    session.run(
        "MATCH (n) CALL { WITH n DELETE n } IN TRANSACTIONS OF 10000 ROWS"
    ).consume()
    for record in session.run("SHOW INDEXES YIELD name RETURN name"):
        session.run(f"DROP INDEX {record['name']} IF EXISTS")


def seed_account(session, update_tag):
    session.run(
        "MERGE (a:AWSAccount{id: $id}) SET a.lastupdated = $tag",
        id=ACCOUNT_ID,
        tag=update_tag,
    )


def make_ec2_instances(n, with_rels_fraction=0.5):
    """Generate realistic EC2 instance dicts matching EC2InstanceSchema property refs."""
    out = []
    for i in range(n):
        has_profile = i % 2 == 0 and with_rels_fraction > 0
        out.append(
            {
                "InstanceId": f"i-{i:017x}",
                "PublicDnsName": f"ec2-{i}.compute.amazonaws.com",
                "PrivateIpAddress": f"10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}",
                "PublicIpAddress": f"54.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}",
                "ImageId": f"ami-{i % 500:012x}",
                "InstanceType": random.choice(["t3.micro", "m5.large", "c5.xlarge"]),
                "MonitoringState": "disabled",
                "State": "running",
                "LaunchTime": "2024-01-01T00:00:00Z",
                "LaunchTimeUnix": 1704067200,
                "IamInstanceProfile": (
                    f"arn:aws:iam::{ACCOUNT_ID}:instance-profile/profile-{i % 200}"
                    if has_profile
                    else None
                ),
                "AvailabilityZone": "us-east-1a",
                "Tenancy": "default",
                "HostResourceGroupArn": None,
                "Platform": None,
                "Architecture": "x86_64",
                "EbsOptimized": True,
                "BootMode": "uefi",
                "InstanceLifecycle": None,
                "HibernationOption": False,
                "EksClusterName": None,
                "ReservationId": f"r-{i % 1000:017x}",
            }
        )
    return out


def timer(label, fn, *args, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    dt = time.perf_counter() - t0
    print(f"  {label}: {dt:.3f}s")
    return result, dt


def rand_suffix(k=6):
    return "".join(random.choices(string.ascii_lowercase, k=k))
