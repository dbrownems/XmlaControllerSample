using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Identity.Client;
using System.Collections.Concurrent;
using System.IdentityModel.Tokens.Jwt;

namespace XmlaControllerSample.Services
{

    public class AdomdConnectionPool : IPooledObjectPolicy<AdomdConnection>
    {
        private IConfidentialClientApplication client;
        private JwtSecurityTokenHandler tokenHandler;
        private ILogger log;
        string clientId;
        string clientSecret;
        string tenantId;
        string dataSource;
        string catalog;

        ObjectPool<AdomdConnection> pool;
        ConcurrentDictionary<AdomdConnection, ConnectionPoolEntry> entries = new ConcurrentDictionary<AdomdConnection, ConnectionPoolEntry>();


        public AdomdConnectionPool(IConfiguration config, ILogger<AdomdConnectionPool> log, IServiceProvider services)
        {
            clientId = config["ClientId"];
            clientSecret = config["ClientSecret"];
            tenantId = config["TenantId"];
            dataSource = config["XMLAEndpoint"];
            catalog = config["DatasetName"];
            tokenHandler = new JwtSecurityTokenHandler();
            this.log = log;

            var authority = $"https://login.microsoftonline.com/{tenantId}";

            client = ConfidentialClientApplicationBuilder.Create(clientId)
                                                         .WithAuthority(authority)
                                                         .WithClientSecret(clientSecret)
                                                         .Build();
            pool = ObjectPool.Create<AdomdConnection>(this);
            

        }
        public AdomdConnection GetConnection()
        {

            var con = pool.Get();
            var entry = GetEntry(con);

            //weed out connections whose tokens have timed out while in the pool
            while (!entry.IsValidForCheckout)
            {
                pool.Return(con);
                con = pool.Get();
            }

            GetEntry(con).RecordCheckOut();
            return con;
        }

        ConnectionPoolEntry GetEntry(AdomdConnection con)
        {
            if (entries.TryGetValue(con, out var entry))
            {
                return entry;
            }
            throw new InvalidOperationException("No ConnectionPoolEntry found for AdodbConnection");
        }

        public void ReturnConnection(AdomdConnection con)
        {
            pool.Return(con);
        }
    

        async Task<string> GetAccessTokenAsync()
        {
        
            //use this resourceId for Power BI Premium
            var scope = "https://analysis.windows.net/powerbi/api/.default";
            
            //use this resourceId for Azure Analysis Services
            //var resourceId = "https://*.asazure.windows.net";

            var token = await client.AcquireTokenForClient(new List<string>() { scope }).ExecuteAsync();

            return token.AccessToken;
        }

        AdomdConnection IPooledObjectPolicy<AdomdConnection>.Create()
        {
            var accessToken = GetAccessTokenAsync().Result;
            var constr = $"Data Source={dataSource};User Id=;Password={accessToken};Catalog={catalog};";
            var con = new AdomdConnection(constr);
            var entry = new ConnectionPoolEntry(con, constr);

            con.Open();
            var token = tokenHandler.ReadJwtToken(accessToken);
            entry.ValidTo = token.ValidTo.ToLocalTime();
            entries.TryAdd(con, entry);
            return con;
        }

        bool IPooledObjectPolicy<AdomdConnection>.Return(AdomdConnection con)
        {
            var entry = GetEntry(con);


            if (entry.ValidTo.Subtract(DateTime.Now) < TimeSpan.FromMinutes(5) || !entry.IsValidForCheckout)
            {
                con.Dispose();
                entries.TryRemove(con, out _);
                return false;
            }
            else
            {
                entry.RecordCheckIn();
                return true;
            }
        }

        class ConnectionPoolEntry
        {

            public ConnectionPoolEntry(AdomdConnection con, string connectionString)
            {
                this.Connection = con;
                this.ConnectionString = connectionString;

                //the combindation of the strong reference to the connection
                //and this delegate ties the reachability of the ConnectionPoolEntry and the AdomdConnection together
                //so they are guaranteed to become unreachable at the same time
                //This would enable the ConnectionPool to keep a WeakReference to the ConnectionPoolEntry without
                //keeping the AdomdConnection alive, but also not worry about the ConnectionPoolEntry being GCd
                //while the AdomdConnection is still alive.
                con.Disposed += (s, a) =>
                {
                    this.IsDisposed = true;
                    this.Connection = null;
                };


            }

            public bool IsValidForCheckout
            {
                get
                {
                    return ValidTo.Subtract(DateTime.Now) > TimeSpan.FromMinutes(1) && !IsDisposed;
                }
            }

            public bool IsDisposed { get; private set; } = false;

            [System.Text.Json.Serialization.JsonIgnore]
            public string ConnectionString { get; private set; }

            [System.Text.Json.Serialization.JsonIgnore]
            public AdomdConnection? Connection { get; private set; }

            public DateTime ValidTo { get; set; }

            public void RecordCheckIn()
            {
                IsCheckedOut = false;
                TotalCheckoutTime += DateTime.Now.Subtract(LastCheckedOut);
                LastCheckedIn = DateTime.Now;


            }

            public void RecordCheckOut()
            {
                IsCheckedOut = true;
                LastCheckedOut = DateTime.Now;
                TimesCheckedOut += 1;
            }
            public bool IsCheckedOut { get; private set; }
            public int TimesCheckedOut { get; private set; } = 0;

            [System.Text.Json.Serialization.JsonIgnore]
            public TimeSpan TotalCheckoutTime { get; private set; }
            public DateTime LastCheckedOut { get; private set; } = DateTime.MinValue;
            public DateTime LastCheckedIn { get; private set; } = DateTime.MinValue;
            public DateTime CreatedAt { get; private set; } = DateTime.Now;

            public override string ToString()
            {
                return System.Text.Json.JsonSerializer.Serialize(this);
            }
        }

    }
}
