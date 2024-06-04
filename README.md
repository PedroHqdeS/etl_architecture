# ETL using Polars

This project performs an ETL (Extract, Transform, Load) process using the
Polars library. The data flow is structured into three basic parts: 
- Source -> Bronze
- Bronze -> Silver
- Silver -> Gold

# Layer Rules
### Bronze

### Silver

### Gold

# Code Structure
The developed code follows the below structure: 
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
- **connectors:** Contains the components responsible for connecting and extracting
data from any external source or reading and writing data in any Data Lake's 
layer in any needed format.
  - **base:** Base code defining the responsibilities and behaviors described above.
  All components that will perform tasks involving reading or writing data and are
  placed inside *connectors* must have the classes in this directory as superclasses.
- **data_lake:** Contains the representation of a general data lake used by data platforms.
- **datasets:** Contains the representation of a source with data to be extracted.
- **layers:** Contains the components responsible for standardizing and structuring
the path patterns for Data Lake's layers.
    - **base:** Base code defining the responsibilities and behaviors described above.
  All components that will perform tasks that involve standardizing data paths for entities
  within the Data Lake and are placed inside *layers* must have the classes in this 
  directory as superclasses.
- **pipelines:** Contains the components responsible for standardizing the data processing
flow through Data Lake's layers as well applying rules and transformations defined for each layer.
  - **base:** Base code defining the responsibilities and behaviors described above.
  All components that will perform tasks that involve data manipulations through the layers and
  are placed inside *pipelines* must have the classes in this directory as superclasses.
- **utils:** Contains helpful and general code that can be used by all components described above.