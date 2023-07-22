
# Airvault: A Data-Vault Framework Powered by Apache Airflow

Airvault represents a robust data-vault framework built on the foundation of Apache Airflow. This framework seamlessly employs templated SQL-scripts alongside custom Airflow operators to proficiently stage and load data into hubs, links, and satellites.

As of now, Airvault exclusively supports PostgreSQL as the underlying database engine. However, due to its SQL-driven nature, the framework can be easily extended to accommodate any SQL-dialect.

# Table of Contents
- [Fundamental Mechanics](#fundamental-mechanics)
- [Installation](#installation)
- [Configuration](#configuration)
	- [Connection](#connection)
- [Raw Vault Configuration (rv)](#raw-vault-configuration-rv)
	- [Hubs](#hubs)
	- [Links](#links)
		- [Transactional Links with Dependent Child Keys](#transactional-links-with-depenednt-child-keys)
	- [Satellites](#satellites)
		- [Transactional Satellite (Tsat)](#transactional-satellite-tsat)
		- [Multi-Active Satellite](#multi-active-satellite)
		- [Regular Satellite](#regular-satellite)
- [Business-Vault Configuration (bv)](#business-vault-configuration-bv)
	- [Example Configuration](#example-configuration)
		- [SQL-based Virtual Sources](#sql-based-virtual-sources)
		- [Managing Source Fields](#managing-source-fields)
		- [Controlling Load Order with Dependencies](#controlling-load-order-with-dependencies)

# Fundamental Mechanics

In essence, the Airvault framework operates with two distinct "staging" areas. The first, referred to internally as the _source_, necessitates housing all unprocessed raw data collected from various original sources. It is the user's responsibility to ensure the appropriate loading of the _source_-schema.

The actual schema used for staging is referred to as the _stage_. This schema entirely remains under the framework's management. When the DAG (Directed Acyclic Graph) is triggered, the framework generates a dedicated table for each source-target pairing. For instance, to create a satellite named _s_customer_ and populate it with data from the source tables _customer_data_ and _user_data_, the staging process will generate two tables in the staging area: _customer_data__s_customer_ and _user_data__s_customer_. In general, the name of each staging table comprises the names of the source table and the target data-vault table, separated by a double underscore.

Notably, the data within these temporary staging tables undergoes a degree of transformation orchestrated by the framework. This is where _soft rules_ come into play. For instance, all data from the sources undergoes a transformation to _varchar_, followed by a whitespace truncation. Additionally, the fields might be renamed in cases where the user defines a mapping in the framework's configuration file. Furthermore, the framework calculates hash keys, concatenates for composite business keys, and manages zero-key treatment. Finally, the framework enriches the data by adding various meta-data fields, including the _load_date_, _record source_, _tenant_, _task_id_, and _collision code_ for the business key.

Ultimately, the data is prepared to facilitate the loading mechanism, making the process predominantly centered on efficiently copying the datasets into the designated data-vault tables.


# Installation

Installing the framework is a straightforward process that involves cloning the repository and executing a simple `docker-compose up -d` command. This will spin up essential containers required to run the framework, including a Postgres database for both user data and internal meta-data managed by Airflow.

The _docker-compose.yaml_ file contains all the necessary environment variables used by the framework, such as the database connection string, credentials, as well as the _maxvarchar_ variable representing the maximum length for the fields of the data-vault tables. Please note that these variables are not exposed for viewing or editing in the Airflow UI.

# Usage

The repository comes with a preconfigured demo project to help you get started quickly. Once all the docker containers are up and running, simply navigate to _localhost:8080_ in your browser. Log in using the username _airflow_ and the password _airflow_. From the Airflow dashboard, trigger the DAG named _init-demo_. This will execute a SQL script that creates the schemas for the source data, the staging area, as well as the data-vault tables. In the next step, the raw data is inserted into the source schema.

After the completion of the _init-demo_ DAG, trigger the _airvault_ DAG. This will stage the source data into the _stage_ schema and subsequently load the data-vault tables in the _vault_ schema, completing the data processing pipeline.

# Configuration

The data vault is modeled using an ordinary YAML file wich is located in `dags/configs/config.yaml`. 
There are three top-level sections: `connection`, `rv` (raw-vault), and `bv` (business-vault).

## Connection

In this section, the basic connection settings are set up as follows:

```yaml
connection:
  host: postgres     # Hostname or IP of the database to connect to
  db: postgres       # Name of the database to connect to
  port: 5432         # Port number
  user: postgres     # Username
  pw: postgres       # Password for the specified username
  schemas:
    source: quelle   # Schema where the raw, extracted data from the operational DB(s) is stored (requires custom ETL loading)
    stage: stage     # Temporary schema managed by the framework (purged before each run)
    edwh: edwh       # Schema managed by the framework for loading data-vault tables
```

1. `host`: The hostname or IP address of the database to connect to.
2. `db`: The name of the database to connect to.
3. `port`: The port number for the database connection.
4. `user`: The username used for authentication to the database.
5. `pw`: The password associated with the specified username.
6. `schemas`: A subsection where the following three mandatory schemas are defined, and all of them must be created by the user:
   - `source`: The schema where the raw, extracted data from the operational database(s) is stored. The extraction process is not handled by the Framework itself, and this schema needs to be loaded with custom ETL processes.
   - `stage`: This schema is managed by the framework and serves as temporary storage, being purged before each run to ensure a clean workspace for data processing.
   - `edwh`: This schema is also managed by the framework and serves as the destination where the data-vault tables are loaded into.

The connection settings define the basic access and schema configuration required for the data-vault operations. Properly configuring this section is crucial for establishing a successful connection to the database and setting up the necessary schemas for data extraction, staging, and loading into the data vault.

# Raw Vault Configuration (rv)

The raw vault configuration consists of three main categories: **hubs**, **links**, and **sats** (satellites). We will start by discussing the **hubs** subsection.

## Hubs

The **hubs** subsection is used to define the hub tables that will be integrated into the raw vault. Each hub is configured with the following properties:
```YAML
hubs:

 h_customer: # desired name for the hub-table
  hk: hk_customer # name for the hash-key in the hub-table
  bk: bk_customer # name for the business-key in the hub-table
     
  src:                        # list of source-tables
   customer:               # name of 1st source-table 
    bkeycode: default   # optional collision code
    tenant: default     # optional tenant-name
    bks:                # list of business-key or business keys in case of composite-key
     - customer_id   # name of 1st part of the composite-bk
     - customer_code # name of 2nd part of the composite-bk 
   client:                 # 2nd source-table
    bks:                # list of business-key(s) for 2nd source
     - client_id     # name of business-key in source-table
 h_staff:                     # 2nd hub-table
  hk: hk_staff             # ...
  bk: bk_staff
 
  src: 
   staff: 
    bks: 
     - staff_id
     - employee_nr
					- employee_nr
```
- **h_customer** and **h_staff** are the names of the hub tables to be created in the raw vault.
    
- **hk** represents the desired name for the hash key in the hub table, which is essentially the hashed version of the **bk** (business key). The hash key is generated using the formula `md5(concat_ws('|', tenant, bkeycode, coalesce(trim(src_bk1), '-1')), coalesce(trim(src_bk2), '-1'), ..., etc.)`. It includes values for multi-tenancy and business-key collision code, if defined.
    
- **bk** represents the desired name for the business key in the hub table. It is composed of the business keys from the source tables and is used to uniquely identify records in the hub.
    
- **src** is a list of source tables for each hub. For instance, **customer** and **client** are the source tables for the **h_customer** hub, while **staff** is the source table for **h_staff**.
    
- **bkeycode** (optional) is a collision code used to avoid conflicts in business keys when multiple source systems are integrated. If not defined, it defaults to _default_.
    
- **tenant** (optional) is used to support multi-tenancy scenarios. If not defined, it defaults to _default_.
    

Each hub table can have one or more source tables. In the example provided, **h_customer** has two source tables (**customer** and **client**), while **h_staff** has one (**staff**). Each source table may have one or more business keys (specified in the **bks** list), which will be concatenated to form the hub's **bk** and **hk**.

By following this configuration structure, you can effectively define and integrate hub tables into your raw vault.

## Links 

The **links** subsection allows you to configure the link tables in the raw vault. Link tables connect multiple hubs and enable more complex data relationships. Below is a possible configuration for the **links** subsection:

```yaml
links:

  l_rental:
    hk: hk_rental
    hks:
      - hk_inventory
      - hk_staff
      - hk_customer

    src:
      rental:
        bkeycode: default
        tenant: default
        bks:
          inventory_id: hk_inventory
          staff_id: hk_staff
          employee_nr: hk_staff
          customer_id: hk_customer

      rentals_old:
        bks:
          inventory_id: hk_inventory
          staff_id: hk_staff
          employee_nr: hk_staff
```

- **l_rental** is the name of the link table to be created in the raw vault.

- **hk** represents the desired name for the hash key in the link table.

- **hks** is a list of the hash keys of the individual hubs that are linked. In this example, the link table connects **hk_inventory**, **hk_staff**, and **hk_customer**.

- **src** is a list of source tables for the link.

For each source table, you need to provide a **bks** subsection to define mappings from the source-table's business keys to the corresponding hash keys of the link table (defined in **hks**). Since link tables connect multiple hubs, the mappings need to be provided in the order specified in **hks** to ensure proper concatenation, especially for composite business keys.

Additionally, you can see an example of **rentals_old** as a source table. In this case, the source table does not provide a mapping for the hash key **hk_customer**. The framework handles this using zero-key treatment, where missing values are replaced by '-1'.

### Transactional Links with Dependent Child Keys

A different configuration can be seen for the second link, **tl_film_actor**:

```yaml
tl_film_actor:
  hk: hk_film_actor
  hks:
    - hk_film
    - hk_actor

  cks:
    - modifytimestamp: ts

  src:
    film_actor:
      bks:
        film_id: hk_film
        actor_id: hk_actor
```

- **tl_film_actor** is the name of the transactional link table, also known as a "non-historized link."

- **hk** represents the desired name for the hash key in the link table.

- **hks** is a list of the hash keys of the hubs that are linked. In this case, the link table connects **hk_film** and **hk_actor**.

- **cks** is a list of dependent child keys for the link table. These keys can be helpful when **bks** alone are not sufficient to uniquely identify a record in the link table. In this example, the **modifytimestamp** field from the source table **film_actor** is mapped to the field **ts** in the link table.

By utilizing the **links** subsection, you can create link tables that efficiently connect multiple hubs, facilitating more advanced data relationships in your raw vault.

## Satellites

The **sats** subsection allows you to configure the satellite tables in the raw vault. The configuration supports three different types of satellites: transactional satellites (tsats), multi-active satellites (msats), and regular satellites. Below is a demonstration of each type:

### Transactional Satellite (Tsat)

```yaml
sats:
  s_rental:
    hk: hk_rental

    cks:
      modifytimestamp: ts

    attrs:
      - rentaltime

    src:
      rental:
        bks:
          - inventory_id
          - staff_id
          - customer_id
        attrs:
          rental_date: rentaltime
```

- **s_rental** is the desired name for the satellite table.

- **hk** represents the desired name for the hash key in the satellite table.

- **cks** is a list of mappings for dependent child keys. In this example, the **modifytimestamp** field from the source table is mapped to **ts** in the satellite table.

- **attrs** is a list of attribute names in the satellite table.

- **src** is a list of source tables for the satellite. Each source table should have a mapping for the attributes defined in the **attrs** list. 

The satellite _s_rental_, is a type known as a "transactional satellite" or "tsat." It contains a **ck** section, and the same mechanics used for the transactional link (_tl_ film_actor) apply here.

The **attrs** section, unique to satellites, defines the set of attributes that the satellite will have. Please note that **attr** occurs in two different contexts. At the top-level, just below the satellite's name definition, we specify the names of the attributes as they should appear in the target satellite table. On the lower level, within the **src** section, a mapping for the attributes from the source table to the satellite table must be provided. This allows us to choose different names for the attributes in the target satellite compared to the source tables.

It's important to mention that there must be a mapping on the lower level for every attribute defined in the top-level **attr** section. In other words, the source table needs to contain all the attributes defined for the satellite table. Unlike business keys, there is no zero-key treatment for attributes. However, if we need to integrate a source that lacks some attributes defined for the target satellite, we can address this using the business-vault, as will be clarified later on.

### Multi-Active Satellite

```yaml
ms_film:
  hk: hk_film

  attrs:
    - film_title
    - film_release_year

  multiactive: true

  src:
    film:
      bks:
        - film_id
      attrs:
        film_title: title
        film_release_year: release_year
```

- **ms_film** is the desired name for the multi-active satellite table.

- **hk** represents the desired name for the hash key in the multi-active satellite table.

- **attrs** is a list of attribute names for the multi-active satellite table.

- **multiactive** is an optional field that, when set to *true*, designates the satellite as multi-active. 

- **src** is a list of source tables for the multi-active satellite. Each source table should have a mapping for the attributes defined in the **attrs** list.

The second satellite, _ms_film_, corresponds to another category of satellites. Notice the optional **multiactive** key set to _true_. By default, this field is set to _false_. Setting it to _true_ makes the satellite a multi-active satellite. Similar to a "tsat," this type of satellite can have multiple "active" records for the same business key simultaneously. However, historization works differently in this case. A comprehensive description of the different types and use-cases of the various data-vault satellites is beyond the scope of this Readme. It is worth highlighting that a multiactive satellite cannot be a "tsat" at the same time. The data-vault modeler must choose which type of satellite table best suits the use-case.

### Regular Satellite

```yaml
s_actor:
  hk: hk_actor

  attrs:
    - actor_first_name
    - actor_last_name

  src:
    actor:
      bks:
        - actor_id
      attrs:
        first_name: actor_first_name
        last_name: actor_last_name
```

- **s_actor** is the desired name for the regular satellite table.

- **hk** represents the desired name for the hash key in the regular satellite table.

- **attrs** is a list of attribute names for the regular satellite table.

- **src** is a list of source tables for the regular satellite. 

Finally, the _s_actor_ satellite is the simplest of all the defined satellites. It is a regular satellite where only a single record per business key can be "active" or "valid" at the same time. It closely resembles the definition of a _hub_, with the addition of the **attrs** section, and it is loaded in a very similar manner.

# Business-Vault Configuration (bv)

This section provides detailed information on configuring the business-vault for your data warehousing needs. While many aspects of the business-vault are similar to the raw-vault configuration, we will focus on the key differences and enhancements.

## Example Configuration

Below is an example of a possible **bv** configuration for a satellite table:

```YAML
bv:                          
  sats:                      
    bv_s_rental:             
      hk: hk_rental          

      attrs:                 
        - rental_date
        - rental_id
        - user_rating
        - customer_city
      src:                    
        business_rule_nr_123: 
          bks:                
            - inventory_id
            - staff_id
            - customer_id        
          attrs:                                      
            - rental_date: rental_date
            - rental_id: rental_id
            - user_rating: user_rating
            - customer_city:customer_city
            
          sql: "SElECT s1.*,                          # custom SQL defines the "virtualized" source
                       s2.city as customer_city   
	            FROM source_schema.rental s1 
	            JOIN vault_schema.bv_ref_zipcodes s2 
	              ON s2.zipcode = s1.zipcode"  
          
        business_rule_nr_456:                         
          bks:
            - inventory_id
            - staff_id
            - customer_id
          attrs:
            - rental_date:rental_date
            - user_rating: user_rating
            - customer_city: customer_city
            
          sql: "SELECT s1.inventory_id,               # custom sql for the 2nd "virtualized" source
                       s1.staff_id, 
                       '-1' as customer_id, 
                       s1.rental_date,
                       'N/A' as user_rating,
                       s2.city as customer_city 
                FROM vault_schema.rv_s_rentals_old s1 
                JOIN vault_schema.bv_ref_zipcodes s2
	              ON s2.zipcode = s1.zipcode"                

      dependencies:                                   # list of dependencies to control load-order
        - load__bv_ref_rentals
    
```

### SQL-based Virtual Sources

The most significant difference in the business-vault configuration is the introduction of SQL-based virtual sources. Instead of referencing static tables, you can now define custom SQL statements that serve as "virtual" sources. These SQL statements are executed by the framework, allowing you to perform transformations, computations, and data cleansing. You can refer to tables in both the source-schema (e.g., *source_schema*) and the data-vault-schema (e.g., *vault_schema*) or even combine several tables using JOINs.

### Managing Source Fields

It's important to note that no "existence-check" is performed for the provided source attributes (**bks** and **attrs**). It is the user's responsibility to handle this within the SQL statement. For example, if a field exists in one virtual source but not in another, the SQL statement must provide hardcoded values for missing fields.

### Controlling Load Order with Dependencies

Another key difference is the addition of the **dependencies** list. Here, you can control the order in which data-vault tables are loaded. In the example above, both virtual sources' SQL statements reference the table *vault_schema.bv_ref_zipcodes*, which is a business-vault table itself. To ensure correct loading, the framework is instructed to wait for *bv_ref_zipcodes* to load before processing *bv_s_rental* by adding *load__bv_ref_rentals* to the **dependencies** list.

The **dependencies** mechanism allows for intricate loading-order control, enabling complex loading scenarios with multiple dependencies. However, it is essential to ensure that the resulting dependency graph is acyclic, and a table cannot be dependent on itself.

By leveraging these new features, the business-vault configuration offers greater flexibility and robustness in handling complex data transformations and loading dependencies within your data warehousing environment.