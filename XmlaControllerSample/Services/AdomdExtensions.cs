using Microsoft.AnalysisServices.AdomdClient;

public static class AdomdExtensions
{

    public static AdomdDataReader ExecuteReader(this AdomdConnection con, string query, params AdomdParameter[] parameters)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = query;
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }
        return cmd.ExecuteReader();
    }
    public static string ExecuteJson(this AdomdConnection con, string query, params AdomdParameter[] parameters)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = query;
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }
        return cmd.ExecuteJson();
    }
    public static async Task ExecuteJsonToStream(this AdomdConnection con, string query, Stream stream, CancellationToken cancel = default(CancellationToken), params AdomdParameter[] parameters)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = query;
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }
        await cmd.ExecuteJsonToStream(stream, cancel);
    }

    public static string ExecuteJson(this AdomdCommand cmd)
    {
        using var rdr = cmd.ExecuteReader();
        var ms = new MemoryStream();
        var encoding = System.Text.Encoding.UTF8;
        rdr.WriteAsJsonToStream(ms, encoding).Wait();

        return encoding.GetString(ms.ToArray());
    }

    public static async Task ExecuteJsonToStream(this AdomdCommand cmd, Stream stream, CancellationToken cancel = default(CancellationToken))
    {
        using var rdr = cmd.ExecuteReader();
        var encoding = System.Text.Encoding.UTF8;
        await rdr.WriteAsJsonToStream(stream, encoding, cancel);
    }

    public static async Task WriteAsJsonToStream(this AdomdDataReader reader, Stream stream, CancellationToken cancel = default(CancellationToken))
    {
        await reader.WriteAsJsonToStream(stream, System.Text.Encoding.UTF8, cancel);
    }
    public static async Task WriteAsJsonToStream(this AdomdDataReader reader, Stream stream, System.Text.Encoding encoding, CancellationToken cancel = default(CancellationToken))
    {

        if (reader == null)
        {
            return;
        }

        using var rdr = reader;

        //can't call Dispose on these without syncronous IO on the underlying connection
        var tw = new StreamWriter(stream, encoding, 1024 * 4, true);
        var w = new Newtonsoft.Json.JsonTextWriter(tw);
        int rows = 0;

        /*
            {
              "informationProtectionLabel": null,
              "results": [
                {
                  "tables": [
                    {
                      "rows": [
                        {
                          "DimScenario[ScenarioKey]": [],
                          "DimScenario[ScenarioName]": []
                        },
                        {
                          "DimScenario[ScenarioKey]": [],
                          "DimScenario[ScenarioName]": []
                        },
                        {
                          "DimScenario[ScenarioKey]": [],
                          "DimScenario[ScenarioName]": []
                        }
                      ],
                      "error": null
                    }
                  ],
                  "error": null
                }
              ],
              "error": null
            }
         * */
        try
        {

            await w.WriteStartObjectAsync(cancel);
            await w.WritePropertyNameAsync("results", cancel);
            await w.WriteStartArrayAsync(cancel);
            await w.WriteStartObjectAsync(cancel);
            await w.WritePropertyNameAsync("tables", cancel);
            await w.WriteStartArrayAsync(cancel);
            await w.WriteStartObjectAsync(cancel);
            await w.WritePropertyNameAsync("rows", cancel);
            await w.WriteStartArrayAsync(cancel);

            while (rdr.Read())
            {
                if (cancel.IsCancellationRequested)
                {
                    throw new TaskCanceledException();
                }
                rows++;
                await w.WriteStartObjectAsync(cancel);
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    string name = rdr.GetName(i);
                    object value = rdr.GetValue(i);

                    await w.WritePropertyNameAsync(name, cancel);
                    await w.WriteValueAsync(value, cancel);
                }
                await w.WriteEndObjectAsync(cancel);

            }

            await w.WriteEndArrayAsync(cancel);
            await w.WriteEndObjectAsync(cancel);
            await w.WriteEndArrayAsync(cancel);
            await w.WriteEndObjectAsync(cancel);
            await w.WriteEndArrayAsync(cancel);
            await w.WriteEndObjectAsync(cancel);

            await w.FlushAsync();
            await tw.FlushAsync();
            await stream.FlushAsync();

        }
        catch (TaskCanceledException)
        {
            throw;
        }

    }

}