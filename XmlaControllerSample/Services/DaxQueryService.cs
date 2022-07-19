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
                throw new ArgumentException("Parameter Names should not start with `@`");

            return new AdomdParameter(name, value);
        }

        public IDataReader ExecuteReader(string query, params AdomdParameter[] parameters)
        {
            if (con == null)
            {
                this.con = pool.GetConnection();
            }

            var cmd = con.CreateCommand();
            cmd.CommandText = query;

            foreach (var parameter in parameters)
            {
                cmd.Parameters.Add(parameter);
            }
            
            int retries = 1;
            while (retries >= 0)
            {
                try
                {
                    var rdr = cmd.ExecuteReader();
                    if (retries == 0)
                    {
                        log.LogWarning("Query succeeded after retry");
                    }
                    return rdr;
                }
                catch (AdomdConnectionException ex)
                {
                    if (retries > 0)
                    {
                        log.LogWarning($"{ex.GetType().Name} {ex.Message} retrying");
                        this.con.Dispose();
                        this.con = pool.GetConnection();
                        cmd.Connection = this.con;
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
