using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Extensions.ObjectPool;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace XmlaControllerSample.Services
{

    public class AdomdConnectionPool : IPooledObjectPolicy<AdomdConnection>
    {
        private readonly ConnectionOptions options;
        private ILogger log;

        ConcurrentDictionary<string, DateTime> sessionStartTimes = new ConcurrentDictionary<string, DateTime>();
        ObjectPool<AdomdConnection> pool;

        public AdomdConnectionPool(IConfiguration config, ILogger<AdomdConnectionPool> log) : this(config.Get<ConnectionOptions>(),log)
        {
        }
        public AdomdConnectionPool(ConnectionOptions options, ILogger<AdomdConnectionPool> log)
        {
            this.options = options;
            options.Validate();
            this.log = log;

            pool = ObjectPool.Create<AdomdConnection>(this);

            Task.Run(() => WarmUp());
        }

        void WarmUp()
        {
            int n = 4;
            log.LogInformation($"Starting warming Connection Pool with {n} connections");
            var cons = new ConcurrentBag<AdomdConnection>();

            for (int i = 0; i < n; i++)
            {
                cons.Add(pool.Get());
            }
            

            foreach (var c in cons)
            {
                pool.Return(c);
            }
            log.LogInformation($"Completed warming Connection Pool with {n} connections");

        }

        /// <summary>
        /// Set limits on connection lifetime.
        /// TOTO: refine time parameters with testing/PG guidance
        /// </summary>
        /// <param name="con"></param>
        /// <returns></returns>
        bool IsSessionValidForCheckIn(AdomdConnection  con)
        {
            var sessionStart = sessionStartTimes[con.SessionID];
            var openFor = DateTime.Now.Subtract(sessionStart);
            var rv = (openFor < TimeSpan.FromMinutes(20));
            return rv;

        }
        bool IsSessionValidForCheckOut(AdomdConnection con)
        {
            var sessionStart = sessionStartTimes[con.SessionID];
            var openFor = DateTime.Now.Subtract(sessionStart);
            var rv = (openFor < TimeSpan.FromMinutes(25));
            return rv;

        }

        /// <summary>
        /// Runs a trivial command on the connection before returning it
        /// </summary>
        /// <returns></returns>
        public AdomdConnection GetValidatedConnection()
        {
            var con = GetConnection();
            int retries = 0;
            while (true)
            {
                var cmd = con.CreateCommand();
                cmd.CommandText = "";
                try
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    cmd.ExecuteNonQuery();

                    log.LogInformation($"Connection validated on checkout in {sw.ElapsedMilliseconds:F0}ms");
                    return con;
                }
                catch (AdomdConnectionException ex)
                {
                    log.LogWarning($"Connection failed validation on checkout {ex.GetType().Name} : {ex.Message}");
                    con.Dispose();
                    con = GetConnection();
                    retries++;
                }
            }
        }
        public AdomdConnection GetConnection()
        {

            var con = pool.Get();

        
            while (con.State != System.Data.ConnectionState.Open || !IsSessionValidForCheckOut(con))
            {
                log.LogInformation("Retrieved connection that either was not Open or whose session has timed out.");
                ReturnConnection(con);
                con = pool.Get();
            }
            
            return con;
        }

        public void ReturnConnection(AdomdConnection con)
        {
            if (con == null)
                return;

            
            pool.Return(con);
        }

       

        AdomdConnection IPooledObjectPolicy<AdomdConnection>.Create()
        {
            var constr = $"Data Source={options.XmlaEndpoint};User Id=app:{options.ClientId}@{options.TenantId};Password={options.ClientSecret};Catalog={options.DatasetName}";
            var con = new AdomdConnection(constr);

            con.Open();
            sessionStartTimes.AddOrUpdate(con.SessionID, s => DateTime.Now, (s,d) => DateTime.Now);
            log.LogInformation("Creating new pooled connection");
                        
            return con;
        }

        bool IPooledObjectPolicy<AdomdConnection>.Return(AdomdConnection con)
        {
            if (con.State != System.Data.ConnectionState.Open || !IsSessionValidForCheckIn(con))
            {
                
                sessionStartTimes.Remove(con.SessionID, out _);
                con.Dispose();
                return false;
            }
                


            return true;
        }

        public class ConnectionOptions
        {
            private bool UseManagedIdentity { get; set; }  //not implemented
            public string ClientId { get; set; }
            public string ClientSecret { get; set; }
            public string TenantId { get; set; }
            public string XmlaEndpoint { get; set; }
            public string DatasetName { get; set; }
            private string EffectiveUserName { get; set; }//not implemented

            public void Validate()
            {
                if (!UseManagedIdentity && (ClientId == null || ClientSecret == null || TenantId == null))
                {
                    throw new ArgumentException("If not Using Manged Identity, ClientId, ClientSecret, and TenantId are required.");
                }
                if (XmlaEndpoint == null || !Uri.IsWellFormedUriString(XmlaEndpoint, UriKind.Absolute))
                {
                    throw new ArgumentException("XmlaEndpoint Uri is required, and must be a valid Uri.");
                }
                if (DatasetName == null)
                {
                    throw new ArgumentException("DatasetName is required.  Specify the name of the Dataset or Database.");
                }
            }


        }
     
  
    }
}
