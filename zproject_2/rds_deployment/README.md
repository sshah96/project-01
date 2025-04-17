## Northwind Data Pipeline with PostgreSQL, AWS-RDS, dbt, and Tableau

This project showcases a cloud-native ELT pipeline where the Northwind dataset is hosted in AWS RDS (PostgreSQL), transformed using dbt, and visualized in Tableau. The pipeline is fully containerized and includes best practices in dimensional modeling, data snapshots, and dbt project structuring.

During data transformation with dbt, these business models and isights were captured and solved:
-- Top Customer Revenue
-- Top Employee Revenue
-- Top Performing Products
-- one OBT
-- Three Dimension Tables
-- One Fact Table
-- Two snapshots to capture SCD2 applied on both Customer and Employee Tables

## Tech Stack
-- Amazon RDS (PostgreSQL): Hosted source and target database

-- dbt (Data Build Tool): SQL-based transformation and modeling

-- Docker & ECS: Containerize and orchestrate the dbt workload

-- pgAdmin: PostgreSQL management interface

-- Tableau: Final data visualization platform

## Project Structure
## Create a project folder in the VScode 
alpaca_rds_dbt/
├── models/
│   └── ...
├── seeds/
│   └── customers.csv
│   └── ...
├── snapshots/
├── dbt_project.yml
├── profiles.yml

## Getting Started
## Clone the GitHub Repository of Northwind 

cd <project-folder>
git clone https://github.com/pthom/northwind_psql.git

## Start PostgreSQL with Docker
## Paste the following into your docker-compose.yml:
version: '3.8'
services:
  db:
    image: postgres:14
    container_name: db
    ports:
      - "5433:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: northwind
    volumes:
      - ./init:/docker-entrypoint-initdb.d

# Start the PostgreSQL container:
docker-compose up -d

## Verify the container is running:
docker ps

## Export CSVs from PostgreSQL
## Access the container:
docker exec -it <container_id_or_name> bash


## Export tables:
COPY customers TO '/tmp/customers.csv' DELIMITER ',' CSV HEADER;
# repeat for other tables (orders, employees, etc.)

## Copy to host:
docker cp db:/tmp/customers.csv ./seeds/customers.csv

## Create and Configure RDS

-- Create a PostgreSQL RDS instance via AWS Console.

-- Modify inbound rules in its security group to allow your IP.

-- Copy endpoint and create new server in pgAdmin.

-- Create the target database.


## dbt Setup
## Initialize a dbt Project
## Create profiles.yml, and paste the profile below
rds_deployment:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <your-rds-endpoint>
      user: postgres
      password: <your-password>
      port: 5432
      dbname: alpaca_rds
      schema: public
      threads: 12

## Load Seed Data
dbt seed --profiles-dir .


## Run Transformations
dbt run --profiles-dir .

## Dockerize the dbt Project
## Build the Docker image:
docker build -t my-dbt-project .


## Deploy with ECR + ECS
## Create ECR Repository
aws ecr create-repository --repository-name my-dbt-project

## Authenticate Docker to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <your-aws-id>.dkr.ecr.<region>.amazonaws.com


## Tag and Push Image
Copy push codes from the ECR


## Launch ECS Cluster and Task 
## Create ECS cluster manually or via CLI:
aws ecs create-cluster --cluster-name dbt-cluster

## In ECS console, define a task using the pushed image.

# Connect AWS-RDS to Tableau
## After dbt has loaded your transformed models into RDS:


## Summary
## This RDS project showcases:

-- Dockerized PostgreSQL for initial dataset export

-- CSV-based loading into AWS RDS via dbt seeds

-- Transformations using dbt

-- Docker image deployment using AWS ECR and ECS 

-- Visualizing in Tableau