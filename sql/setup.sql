--------------------------------------------------------------------------------
-- Application Role
--------------------------------------------------------------------------------
use role useradmin;
create role if not exists snowguide_role;
grant role snowguide_role to role sysadmin;
grant role snowguide_role to user jbarnett;
use role accountadmin;
GRANT BIND SERVICE ENDPOINT on account to role snowguide_role;

--------------------------------------------------------------------------------
-- Database and schema, warehouse, compute pool
--------------------------------------------------------------------------------
use role sysadmin;
create database if not exists snowguide;
create schema if not exists snowguide.snowguide;
grant all on database snowguide to role snowguide_role;
grant ownership on schema snowguide.snowguide to role snowguide_role;;
create warehouse if not exists snowguide_wh warehouse_size = large;
grant all on warehouse snowguide_wh to role snowguide_role;

use role snowguide_role;
use schema snowguide.snowguide;
CREATE IMAGE REPOSITORY IF NOT EXISTS snowguide.snowguide.snowguide_repository;
show image repositories;
show image repositories like 'snowguide_repository%' ->> select concat('docker login ',"repository_url") as login from $1;

CREATE STAGE IF NOT EXISTS snowguide.snowguide.data
  DIRECTORY = ( ENABLE = true );

--------------------------------------------------------------------------------
-- Network 
--------------------------------------------------------------------------------
use role accountadmin;
DROP NETWORK RULE IF EXISTS slack_network_rule;
CREATE NETWORK RULE IF NOT EXISTS slack_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('0.0.0.0:443', 'api.slack.com:443', 'slack.com:443');
 
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS slack_apis_access_integration;
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS slack_apis_access_integration
  ALLOWED_NETWORK_RULES = (slack_network_rule)
  ENABLED = true;
grant usage on integration slack_apis_access_integration to role snowguide_role;

CREATE COMPUTE POOL IF NOT EXISTS snowguide_compute_pool
  MIN_NODES = 1
  MAX_NODES = 3
  INSTANCE_FAMILY = CPU_X64_XS;
grant usage on compute pool snowguide_compute_pool to role snowguide_role;