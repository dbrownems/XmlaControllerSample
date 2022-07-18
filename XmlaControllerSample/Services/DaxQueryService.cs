using Microsoft.AnalysisServices.AdomdClient;
using System.Data;

namespace XmlaControllerSample.Services
{
    public sealed class DaxQueryService : IDisposable
    {
        private AdomdConnectionPool pool;
        private ILogger<DaxQueryService> log;
        private AdomdConnection con;

        public DaxQueryService(AdomdConnectionPool pool, ILogger<DaxQueryService> log)
        {
            this.pool = pool;
            this.log = log;

        }

        public AdomdParameter CreateParameter(string name, object value)
        {
            if (name.StartsWith("@"))
                name = "@" + name;

            return new AdomdParameter(name, value);
        }

        public IDataReader ExecuteReader(string query, params AdomdParameter[] parameters)
        {
            int retries = 1;
            while (retries >= 0)
            {
                try
                {
                    if (con == null)
                    {
                        this.con = pool.GetConnection();
                    }
                    //con.ChangeEffectiveUser(effectiveUserName);
                    var cmd = con.CreateCommand();
                    foreach (var parameter in parameters)
                    {
                        cmd.Parameters.Add(parameter);
                    }

                    cmd.CommandText = query;
                    return cmd.ExecuteReader();
                }
                catch (AdomdConnectionException ex)
                {
                    if (retries > 0)
                    {
                        log.LogInformation($"{ex.GetType().Name} {ex.Message} retrying");
                        retries -= 1;
                        continue;

                    }
                    throw;
                    
                }
            }
            throw new InvalidOperationException("Unexpected code flow");

        }

        public void Dispose()
        {
            pool.ReturnConnection(con);
        }
    }
}
