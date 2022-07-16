# XmlaControllerSample

This is a sample .NET 6 Web API project that sends a DAX query to a Power BI Premium (or Azure Analysis Services) XML/A Endpoint and returns the results to the client as JSON.  It uses a strongly typed controller and a model class to represent the query results.  The incoming data is mapped to the model type using Dapper.  The intent of this sample is to demonstrate how to send a DAX query and return data to a client in a typical .NET 6 Web Application.

There's also a custom Connection Pool for AdodbConnection objects, as there is no built-in connection pooling, but connection startup is expensive.  The connection pool is used by a Scoped Service class called DaxQueryService.  DaxQueryService will check-out and check-in connections to the connection pool, and the DI framework will ensure that the connection is returned to the pool at the end of the Http Request.

The required configuration parameters are:
```
  "ClientId": "<ClientID of a Service Principal (aka App Registration) with access to the Dataset>",
  "ClientSecret": "<ClientSecret for the Service Principal>",
  "TenantId": "<TenantID to authenticate to>",
  "DatasetName": "<Name of the target Dataset>",
  "XMLAEndpoint": "powerbi://api.powerbi.com/v1.0/myorg/<YourWorkspaceName>",
```
These can be provided in appsettings.json, user secrets in development, or through environment variables in a production deployment.  The Service Principal needs to be added to the Power BI Workspace, but does not need any additional Azure Active Directory permissions.
 
This sample currently does not demonstrate setting EffectiveUserName, or using an on-behalf-of authentication flow.

