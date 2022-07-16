using Microsoft.AnalysisServices.AdomdClient;
using System.Data;

namespace XmlaControllerSample.Services
{
    public sealed class DaxQueryService : IDisposable
    {
        private AdomdConnectionPool pool;
        private AdomdConnection con;

        public DaxQueryService(AdomdConnectionPool pool)
        {
            this.pool = pool;
            this.con = pool.GetConnection();
        }

        public IDataReader ExecuteReader(string query)//, string? effectiveUserName = null)
        {
            //con.ChangeEffectiveUser(effectiveUserName);
            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            return cmd.ExecuteReader();
        }
        
        public void Dispose()
        {
            pool.ReturnConnection(con);
        }
    }
}
