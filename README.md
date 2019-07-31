# spectrum
Experimenting with quasi standard steps in the software development architecture and lifecycle continuum

The areas we plan to explore are:
- simplified local development of the entire stack
- database
- API
- Application front end
- Infrastructure deployment
- CICD integration across the entire stack

## Stack Tech
- Application
  - PostgreSQL
  - GoLang API
  - React/Angular/?
  - Logging
- Infrastructure
  - Tools:
    - Terraform
    - Ansible / [Cloud init](https://cloudinit.readthedocs.io/en/latest/) / ?
  - Monitoring
    - Datadog, ?
  - Logging
  - Secrets Management
  - Notifications

## Database
- Dataset
  - https://github.com/jldbc/coffee-quality-database
- To do:
  - [x] Pull dataset into repo
  - [x] Create a database schema for the dataset
    - indexes, foreign keys, etc
    - https://dbdiagram.io/
  - [ ] Create makefile for running various commands
  - [ ] Create dockerfile with postgres and libraries
  - [ ] Script to:
    - [ ] build the database and tables
    - [ ] import the data
- Resources
  - A Quick-Start Tutorial on Relational Database Design
    - https://www.ntu.edu.sg/home/ehchua/programming/sql/relational_database_design.html