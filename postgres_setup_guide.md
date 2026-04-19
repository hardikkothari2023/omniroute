# PostgreSQL Setup Guide for OmniRoute

This guide provides the necessary commands to install and configure PostgreSQL on your Ubuntu EC2 instance based on your required configuration:
- **Database Name**: `omniroute_dwh`
- **User**: `postgres`
- **Password**: `postgres`
- **Port**: `5432`

## 1. Install PostgreSQL

First, update your package lists and install the PostgreSQL server along with additional utilities:

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib
```

Start the PostgreSQL service and enable it to run automatically on system boot:

```bash
sudo systemctl enable postgresql
sudo systemctl start postgresql
```

## 2. Configure the Database, Schema, and Tables

PostgreSQL is installed with a default superuser named `postgres`. We will set its password, create the target database, and set up the schema and required table.

Run the following block directly in your terminal. It connects using the `postgres` user and executes all SQL statements in one go:

```bash
sudo -u postgres psql << 'EOF'

-- Update password and create database
ALTER ROLE postgres WITH PASSWORD 'postgres';
CREATE DATABASE omniroute_dwh;
GRANT ALL PRIVILEGES ON DATABASE omniroute_dwh TO postgres;

-- Connect to the new database
\c omniroute_dwh;

-- Create the required schema and table
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.driver_safety_status (
    driver_id VARCHAR(50) PRIMARY KEY,
    base_rate NUMERIC(10,2),
    strike_count INT,
    current_adjusted_rate NUMERIC(10,2),
    status VARCHAR(20),
    month VARCHAR(10)
);

EOF
```

## 3. Configure Local Password Authentication

Depending on the Ubuntu version, PostgreSQL might use `peer` authentication by default for local connections, which can prevent password-based logins via the app. 

To ensure the application can authenticate via password over localhost, you can explicitly set `md5` authentication for local TCP connections.

If your application runs into authentication issues, check the `pg_hba.conf` file (typically located in `/etc/postgresql/16/main/pg_hba.conf` or similar depending on the version):

```bash
sudo nano /etc/postgresql/$(ls /etc/postgresql)/main/pg_hba.conf
```
Ensure there's a line that looks like this for IPv4 local connections:
```text
host    all             all             127.0.0.1/32            md5
```
*(If you make changes, restart the service with `sudo systemctl restart postgresql`)*

## 4. Verify the Connection

You can verify that your database and user are set up correctly by attempting to connect to the new database:

```bash
psql -h localhost -U postgres -d omniroute_dwh -p 5432
```
*(When prompted for a password, enter: `postgres`)*

---
**Note:** Your application configuration will now automatically connect using the variables defined in your code (`PG_HOST=localhost`, `PG_PORT=5432`, `PG_DB=omniroute_dwh`, etc.).
