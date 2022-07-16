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

        public AdomdParameter CreateParameter(string name, object value)
        {
            if (name.StartsWith("@"))
                name = "@" + name;

            return new AdomdParameter(name, value);
        }

        public IDataReader ExecuteReader(string query, params AdomdParameter[] parameters )
        {
            //con.ChangeEffectiveUser(effectiveUserName);
            var cmd = con.CreateCommand();
            foreach ( var parameter in parameters )
            {
                cmd.Parameters.Add( parameter );
            }
            
            cmd.CommandText = query;
            return cmd.ExecuteReader();
            
        }
        
        public void Dispose()
        {
            pool.ReturnConnection(con);
        }
    }
}
