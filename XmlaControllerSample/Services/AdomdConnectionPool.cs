using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Extensions.ObjectPool;
using System.Collections.Concurrent;

namespace XmlaControllerSample.Services
{

    public class AdomdConnectionPool : IPooledObjectPolicy<AdomdConnection>
    {
       // private IConfidentialClientApplication client;
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
            int n = 10;
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

        bool IsSessionValid(AdomdConnection  con)
        {
            var sessionStart = sessionStartTimes[con.SessionID];
            var openFor = DateTime.Now.Subtract(sessionStart);
            var rv = (openFor < TimeSpan.FromMinutes(20));
            return rv;

        }
        public AdomdConnection GetConnection()
        {

            var con = pool.Get();

            while (con.State != System.Data.ConnectionState.Open || !IsSessionValid(con))
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

        

        //async Task<string> GetAccessTokenAsync()
        //{

        //    var sw = new Stopwatch();
        //    sw.Start();
        //    //use this resourceId for Power BI Premium
        //    var scope = "https://analysis.windows.net/powerbi/api/.default";
            
        //    //use this resourceId for Azure Analysis Services
        //    //var resourceId = "https://*.asazure.windows.net";

        //    var token = await client.AcquireTokenForClient(new List<string>() { scope }).ExecuteAsync();

        //    if (sw.ElapsedMilliseconds > 200)
        //        log.LogWarning($"AcquireTokenForClient returned in {sw.ElapsedMilliseconds:F0}ms");

        //    return token.AccessToken;
        //}


        AdomdConnection IPooledObjectPolicy<AdomdConnection>.Create()
        {
            //var accessToken = GetAccessTokenAsync().Result;
            //var constr = $"Data Source={options.XmlaEndpoint};User Id=;Password={accessToken};Catalog={options.DatasetName}";
            var constr = $"Data Source={options.XmlaEndpoint};User Id=app:{options.ClientId}@{options.TenantId};Password={options.ClientSecret};Catalog={options.DatasetName}";
            var con = new AdomdConnection(constr);

            con.Open();
            sessionStartTimes.AddOrUpdate(con.SessionID, s => DateTime.Now, (s,d) => DateTime.Now);
            log.LogInformation("Creating new pooled connection");
                        
            return con;
        }

        bool IPooledObjectPolicy<AdomdConnection>.Return(AdomdConnection con)
        {
            if (con.State != System.Data.ConnectionState.Open || !IsSessionValid(con))
            {
                sessionStartTimes.Remove(con.SessionID, out _);
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
