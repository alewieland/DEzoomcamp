## DBT Project 
for this section, we're working from dbt cloud. We ill need to first create and configure the project with the following:
* define a name for our project ;
* configure the connection with a datawarehouse (BigQuery in this case) ;
* configure the connection with a code supervision repo (github in our case).

Once it is done, not that dbt do not allow to work on the main branch, so we will need to create another branch dedicated to our project. 
Also, the repo is synced automaticaly with the github.

### Initialise the project
Once we created our branch, we can **initialize the project**. This will create a folder dedicated to the project into the github repo provided.
A lot of files a created during the initialisation, but we will put our focus on **dbt_project.yml** first, where we will change the configuration
so it suits our needs (changing the project name for example).

### Modular data modeling
dbt allows us to transform our raw data for analytics purposes. It does so with a modular data modeling approach. We are going to build tables like : 
* facts ;
* dimensions.

Going from our sources tables, we will create some sql scripts called models in dbt, that are going to be doing transformation of our data. We will add
some scripts to clean the data, doing some of those operations : 
* deduplication ;
* renaming ; 
* type casting ;
* adding business logic ;
* transforming it into facts and dimension table.

At the end, we are going to add the clean data into our datamarts to serve the stakeholders or end users.

#### Anatomy of a dbt model
A dbt model is a sql script, written into a file. The name of the file will always correspond to the name of the model.
dbt will use this file to create the DDL for us, but we have to tell dbt how to create that DDL model. For example, specifying that 
we need a materialized table into the database, instead of a view. There are 4 types of table that can be created by dbt models :
* **Ephemeral** - exists only for a single dbt run (it is a sort of temporary table) ;
* **View** - virtual table created by dbt, that can be queried like regular table ;
* **Table** - physical representation of data that are created and stored into the database ;
* **Incremental** - tables that allow updates into an existing table, reducing the need for full data refreshes.

#### Macros
A macro in dbt works like a function in traditional programming. It's created using jinja templating language and helps in elements such as :
* the use of control structure in SQL (for loops for example) ;
* the use of environment variables in dbt project for production deployments ;
* operate on the result of a query to generate another query ;
* etc ...

It allows users to encapsulate specific, often repetitive, SQL logic into a named, callable unit ([source](https://popsql.com/blog/dbt-macros)).

#### variables
Variables are useful for defining values that should be used across the project. A variable can also be called in a command.