using Microsoft.AnalysisServices.AdomdClient;
using Polly;
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
                throw new ArgumentException("Parameter Names should not start with '@'");

            return new AdomdParameter(name, value);
        }

        public IDataReader ExecuteReader(string query, params AdomdParameter[] parameters)
        {
            if (con == null)
                con = pool.GetConnection();

            var retryPolicy = Policy.Handle<AdomdConnectionException>()
                                    .WaitAndRetry(new TimeSpan[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4) },
                                    onRetry: (e, t) => 
                                    {
                                        log.LogWarning($"Retrying after exception {e.GetType().Name} : {e.Message}");
                                        //retry using a new, validated connection
                                        con.Dispose();
                                        this.con = pool.GetValidatedConnection(); 
                                    });

            return retryPolicy.Execute(() => ExecuteReaderImpl(query, parameters));
        }
        IDataReader ExecuteReaderImpl(string query, params AdomdParameter[] parameters)
        {
            var cmd = con.CreateCommand();
            cmd.CommandText = query;

            foreach (var parameter in parameters)
            {
                cmd.Parameters.Add(parameter);
            }
            var rdr = cmd.ExecuteReader();
            return rdr;
        }

        public void Dispose()
        {
            pool.ReturnConnection(con);
        }
    }
}
