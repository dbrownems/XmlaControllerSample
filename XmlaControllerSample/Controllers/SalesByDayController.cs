using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using XmlaControllerSample.Services;
using XmlaControllerSample.Models;
using Dapper;
using System.Diagnostics;

namespace XmlaControllerSample.Controllers
{


    [Route("api/[controller]")]
    [ApiController]
    [Produces("application/json")]
    public class SalesByDayController : ControllerBase
    {
        string query = @"
        // DAX Query
DEFINE
  VAR __DS0FilterTable = 
    TREATAS({@FiscalYear}, 'DimDate'[FiscalYear])

  VAR __DS0Core = 
    SUMMARIZECOLUMNS(
      'DimDate'[FullDateAlternateKey],
      __DS0FilterTable,
      ""SumSalesAmount"", CALCULATE(SUM('FactInternetSales'[SalesAmount]))
    )


EVALUATE
  __DS0Core

ORDER BY
  'DimDate'[FullDateAlternateKey]
        ";
        private DaxQueryService queryService;
        private ILogger<SalesByDayController> logger;

        public SalesByDayController( DaxQueryService queryService, ILogger<SalesByDayController> logger)
        {
            this.queryService = queryService;
            this.logger = logger;
        }
        static SalesByDayController()
        {
            var map = new CustomPropertyTypeMap(
                typeof(SalesByDay),
                (type, columnName) =>
                {
                    if (columnName == "DimDate[FullDateAlternateKey]")
                    {
                        return type.GetProperty(nameof(SalesByDay.Date));
                    }

                    if (columnName == "[SumSalesAmount]")
                    {
                        return type.GetProperty(nameof(SalesByDay.SalesAmount));
                    }

                    throw new InvalidOperationException($"No matching mapping for {columnName}");
                }
            );

            Dapper.SqlMapper.SetTypeMap(typeof(SalesByDay), map);
        }


        [HttpGet]
        public async Task<List<SalesByDay>> GetSalesByDay(int fiscalYear)
        {

            var sw = new Stopwatch();
            sw.Start();
            var pFy = queryService.CreateParameter("FiscalYear", fiscalYear);
            using var rdr = queryService.ExecuteReader(query, pFy);
            var sales = rdr.Parse<SalesByDay>().ToList();
            logger.LogInformation($"Query executed in {sw.Elapsed.TotalSeconds:F2}sec returning {sales.Count}rows");

            return sales; 


        }
    }
}
