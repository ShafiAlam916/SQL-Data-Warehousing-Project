/*
=============================================================
Schema Initialization Script
=============================================================
Script Objective:
    This script prepares the project structure by creating 
    the 'bronze', 'silver', and 'gold' schemas. 

CAUTION:
    Executing this script will permanently delete all objects 
    and data inside the 'bronze', 'silver', and 'gold' schemas 
    if they already exist. Ensure backups are taken before running.
*/

-- Drop schemas if they already exist
DROP SCHEMA IF EXISTS bronze;
DROP SCHEMA IF EXISTS silver;
DROP SCHEMA IF EXISTS gold;

-- Recreate schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;