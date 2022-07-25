using Microsoft.AnalysisServices.AdomdClient;
using Polly;
using System.Data;
using System.Diagnostics;

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

        public async Task ExecuteJSONToStream(string query, Stream target, params AdomdParameter[] parameters)
        {
            using var rdr = ExecuteReader(query, parameters);
            await rdr.WriteAsJsonToStream( target, CancellationToken.None);
        }

        public string ExecuteJSON(string query, params AdomdParameter[] parameters)
        {
            
            var ms = new MemoryStream();
            ExecuteJSONToStream(query, ms, parameters).Wait();
            ms.Position = 0;
            return System.Text.Encoding.UTF8.GetString(ms.ToArray());
        }

        public AdomdDataReader ExecuteReader(string query, params AdomdParameter[] parameters)
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

            var result =  retryPolicy.ExecuteAndCapture(() => ExecuteReaderImpl(query, parameters));
            if (result.Outcome == OutcomeType.Successful)
            {
                return result.Result;
            }
            else
            {
                con.Dispose();
                con = null;
                throw result.FinalException;
            }
        }
        AdomdDataReader ExecuteReaderImpl(string query, params AdomdParameter[] parameters)
        {
            var sw = new Stopwatch();
            sw.Start();
            var rdr = con.ExecuteReader(query, parameters);
            if (sw.ElapsedMilliseconds > 2000)
            {
                log.LogWarning($"ExecuteReader completed in {sw.ElapsedMilliseconds}ms");
            }
            return rdr;
        }


        public void Dispose()
        {
            if (con != null)
                pool.ReturnConnection(con);
        }
    }
}
