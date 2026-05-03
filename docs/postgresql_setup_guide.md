# PostgreSQL Setup Guide — EC2 (Same Instance as Airflow)

## 1. Install PostgreSQL on Your EC2

```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install PostgreSQL 15
sudo apt install -y postgresql postgresql-contrib

# Start and enable the service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Verify it's running
sudo systemctl status postgresql
```

## 2. Create the OmniRoute Reporting Database

```bash
# Switch to postgres user
sudo -u postgres psql

# Inside psql, run:
CREATE USER omniroute_user WITH PASSWORD 'OmniRoute2026!';
CREATE DATABASE omniroute_reporting OWNER omniroute_user;
GRANT ALL PRIVILEGES ON DATABASE omniroute_reporting TO omniroute_user;

# Exit psql
\q
```

## 3. Configure PostgreSQL for Network Access

By default, PostgreSQL only accepts local connections. Since Glue jobs run on AWS-managed infrastructure (not your EC2), you need to allow connections from the Glue VPC.

```bash
# Edit pg_hba.conf to allow password auth
sudo nano /etc/postgresql/*/main/pg_hba.conf

# Add this line at the bottom (allow connections from any IP with password):
host    omniroute_reporting    omniroute_user    0.0.0.0/0    md5

# Edit postgresql.conf to listen on all interfaces
sudo nano /etc/postgresql/*/main/postgresql.conf

# Change this line:
# listen_addresses = 'localhost'
# To:
listen_addresses = '*'

# Restart PostgreSQL
sudo systemctl restart postgresql
```

> **SECURITY NOTE**: In production, restrict `0.0.0.0/0` to your VPC CIDR (e.g., `10.0.0.0/16`).
> Also ensure your EC2 Security Group allows inbound TCP on port **5432** from the Glue VPC.

## 4. EC2 Security Group — Allow Port 5432

Go to AWS Console → EC2 → Your Instance → Security Groups → Edit Inbound Rules:

| Type | Protocol | Port | Source |
|------|----------|------|--------|
| Custom TCP | TCP | 5432 | Your VPC CIDR (e.g., `10.0.0.0/16`) or `0.0.0.0/0` for testing |

## 5. Test the Connection from EC2

```bash
# Test local connection
psql -h localhost -U omniroute_user -d omniroute_reporting
# Enter password: OmniRoute2026!

# Run a test query
SELECT version();
\q
```

## 6. How Glue Jobs Connect to PostgreSQL

### Option A: Glue JDBC Connection (Recommended)

1. Go to **AWS Glue Console → Connections → Create Connection**
2. Connection type: **JDBC**
3. JDBC URL: `jdbc:postgresql://<YOUR_EC2_PRIVATE_IP>:5432/omniroute_reporting`
4. Username: `omniroute_user`
5. Password: `OmniRoute2026!`
6. VPC: Same VPC as your EC2
7. Subnet: Same subnet as your EC2
8. Security Group: A security group that allows outbound to port 5432

### Option B: Spark JDBC in Code (What We'll Use)

In the Glue script, use Spark's JDBC writer directly:

```python
# Connection properties
jdbc_url = "jdbc:postgresql://<EC2_PRIVATE_IP>:5432/omniroute_reporting"
connection_properties = {
    "user": "omniroute_user",
    "password": "OmniRoute2026!",
    "driver": "org.postgresql.Driver"
}

# Write a DataFrame to PostgreSQL
df.write.jdbc(
    url=jdbc_url,
    table="report.fuel_efficiency_audit",
    mode="overwrite",  # or "append"
    properties=connection_properties
)
```

> **IMPORTANT**: AWS Glue 4.0 includes the PostgreSQL JDBC driver by default.
> The Glue job must run in the **same VPC** as your EC2 for network access.

### Glue Job Parameters for PostgreSQL

When running the gold-to-postgres Glue job, pass these parameters:

```
--pg_host       <YOUR_EC2_PRIVATE_IP>
--pg_port       5432
--pg_database   omniroute_reporting
--pg_user       omniroute_user
--pg_password   OmniRoute2026!
```

## 7. Find Your EC2 Private IP

```bash
# Run on your EC2
curl http://169.254.169.254/latest/meta-data/local-ipv4
```

Use this IP in the JDBC URL (NOT the public IP — Glue runs inside VPC).

## Summary

| Item | Value |
|------|-------|
| Database | `omniroute_reporting` |
| User | `omniroute_user` |
| Password | `OmniRoute2026!` |
| Port | `5432` |
| Host | Your EC2 private IP |
| Schema | `report` (we'll create this) |
