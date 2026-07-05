Ukiel is a lake. Here, a data lake. In real world it's a lake in Olsztyn, Poland.

The idea:

1. Using Apache DataFusion as query engine.
2. Use Parquet as on disk data format.
3. When inserting data, create files using defined table schema but optimize the Parquet files.
4. Create a custom catalog to manage files and support efficient paritioning
5. what we want to achieve is either multitenancy at file managment level or scalable table-tenancy
   (meaning having table customized per tenant)
6. Catalog must support MV -> events on new data parts
7. Replace on catalog files
8. Q: how to handle deletes? as the database would be multitenant, we could have a worker rewriting the data, that's all.
9. conflicts between: mutations and merges

