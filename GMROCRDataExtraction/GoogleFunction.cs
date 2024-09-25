using Google.Cloud.BigQuery.V2;
using Google.Cloud.Functions.Framework;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace GMROCRDataExtraction
{
    internal class GoogleFunction : IHttpFunction
    {
        private readonly BigQueryClient _bigQueryClient;

        public GoogleFunction()
        {
            // Initialize the BigQuery client
            _bigQueryClient = BigQueryClient.Create("sincere-charmer-436206-j3");
        }

        public async Task HandleAsync(HttpContext context)
        {
            try
            {
                string query = "SELECT * FROM `sincere-charmer-436206-j3.gmr_testing.gmr_json_files`";

                // Execute the query
                var result = await _bigQueryClient.ExecuteQueryAsync(
                                                                     query,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     default
                                                                     );

                var rows = new List<Dictionary<string, object>>();

                foreach (var row in result)
                {
                    var dict = new Dictionary<string, object>();
                    foreach (var field in row.Schema.Fields)
                    {
                        dict[field.Name] = row[field.Name];
                    }
                    rows.Add(dict);
                }

                foreach (var data in rows)
                {
                    if (data.TryGetValue("details", out object valor))
                    {

                        JObject[] obj = JsonConvert.DeserializeObject<JObject[]>((string)valor);

                        JObject firstData = obj[0];

                        string mailAddressStreet = (string)firstData["value"]["17"];
                        string city = (string)firstData["value"]["14"];
                        string postalCode = (string)firstData["value"]["18"];
                        string state = (string)firstData["value"]["16"];
                        string contactNumber = (string)firstData["value"]["0"];
                        string email = (string)firstData["value"]["4"];
                        string primaryFirstName = (string)firstData["value"]["2"];
                        string primaryLastName = (string)firstData["value"]["1"];
                        string primaryDateOfBirth = (string)firstData["value"]["22"];
                        string planCodeId = (string)firstData["value"]["20"];
                        string getCodeId = (string)firstData["value"]["19"];
                        string trackCodeId = (string)firstData["value"]["21"];

                        await context.Response.WriteAsJsonAsync($"{postalCode} {contactNumber} {primaryFirstName} {primaryLastName} {planCodeId}");

                    }
                }
            }
            catch (Exception ex)
            {
                await context.Response.WriteAsJsonAsync(ex.Message);
            }
        }
    }
}
