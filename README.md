<p align="justify">

# ETL using Polars
This project performs an exemplification of an ETL (Extract, Transform, Load)
process on data using the Polars library. For this, data is extracted from some
source, in this case a local source to exemplify, transformed applying some
rules to enhance it and then stored ready for being consumed.

During the transformation process, data goes through some layers into which the
Data Lake is divided. Thus, the ETL flow is structured into three basic parts: 
- Source       -> Bronze Layer
- Bronze Layer -> Silver Layer
- Silver Layer -> Gold Layer

Each layer have their own rules, which are explained in the next section. The
main objective of this process is making available in the last layer reliable
data that can be used for the final user.

In the last section of this documentation the structure of the developed code,
as well as its modules division, that together will perform all the ELT process
is explained.

## Layer Rules
In order to have a structured and standardized Data Lake, which is reliable for
data consuming, it was defined splitting the data treatment and enhancement in
some layers, defining the rules must be applied in data stored there.

The layers and rules for each one follows the below structure:

### Bronze
- It must receive the raw data coming from the source;
- In Bronze, no rules or transformations should be applied, i.e, Bronze data
must be a mirror of what is in the source;
- We must have the same vision as data in the source, for instance, if the
source shows data in a tabular form (Relational databases, Excel, etc.) data
must be stored in a structured form like *parquet* type;
- As data is processed in an incremental way according to a reference date,
Bronze data should be partitioned by this reference date (year, month and day).
However, it is probably that we do not have a cleaned date attribute to apply
the partitioning operation. So, to avoid applying any transformation in data
here the partitioning must occurs in the object path in Data Lake.
### Silver
- Receives data coming from Bronze Layer;
- Data in Silver must be totally structured;
- As data is processed in an incremental way according to a reference date,
Silver data should be partitioned by this reference date (year, month and day);
- Control fields must be created: *Surrogate Key* and *Load at* date;
- Data types casting.
### Gold
- Receives data coming from Silver Layer;
- All records in Gold Layer must be in its latest version (Snapshot vision),
i.e., Gold represents the current state of data, where data is clean, reliable
and ready to use.

## Code Structure
The developed code to perform the ETL process follows the below structure: 
```
└── etl_architecture
|    └── connectors
|        └── base
|    └── data_lake
|         └── bronze
|         └── silver
|         └── gold
|    └── datasets
|    └── layers
|        └── base
|    └── pipelines
|        └── base
|    └── utils
```
- **connectors:** Contains the components responsible for connecting and
extracting data from any external source or reading and writing data in any
Data Lake's layer in any needed format.
  - **base:** Base code defining the responsibilities and behaviors described
  above.
  All components that will perform tasks involving reading or writing data and
  are placed inside *connectors* must have the classes in this directory as
  superclasses.
- **data_lake:** Contains the representation of a general data lake used by
data platforms.
- **datasets:** Contains the representation of a source with data to be
extracted.
- **layers:** Contains the components responsible for standardizing and
structuring the path patterns for Data Lake's layers.
    - **base:** Base code defining the responsibilities and behaviors described
  above. All components that will perform tasks that involve standardizing data
  paths for entities within the Data Lake and are placed inside *layers* must
  have the classes in this directory as superclasses.
- **pipelines:** Contains the components responsible for standardizing the data
processing flow through Data Lake's layers as well as applying rules and
transformations defined for each layer.
  - **base:** Base code defining the responsibilities and behaviors described
  above. All components that will perform tasks that involve data manipulations
  through the layers and are placed inside *pipelines* must have the classes in
  this directory as superclasses.
- **utils:** Contains helpful and general code that can be used by all
components described above.

</p>