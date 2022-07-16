# XmlaControllerSample

This is a sample .NET 6 Web API project that sends a DAX query to a Power BI (or AAS) XML/A Endpoint and returns the results to the client as JSON.  It uses a strongly typed controller and a model class to represent the query results.  The incoming data is mapped to the model type using Dapper.

There's also a custom Connection Pool for AdodbConnection objects, as there is no built-in connection pooling, but connection startup is expensive.  The connection pool is used by a Scoped Service class called DaxQueryService.  DaxQueryService will check-out and check-in connections to the connection pool, and the DI framework will ensure that the connection is returned to the pool at the end of the Http Request.
