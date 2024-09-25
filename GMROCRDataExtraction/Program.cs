using GMROCRDataExtraction.Utils;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using GMROCRDataExtraction.Entities;
using System.Text;

Credentials cr = new Credentials();

Console.WriteLine("Starting process");

try
{
    GoogleCredential credential = GoogleCredential.FromFile(cr.bigqueryAccessPath);

    var client = BigQueryClient.Create(cr.projectId, credential);

    string query = "SELECT * FROM `sincere-charmer-436206-j3.gmr_testing.gmr_json_files`";

    var result = client.ExecuteQuery(query, parameters: null);

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

    //string json = JsonConvert.SerializeObject(rows[0].Values, Formatting.Indented);

    foreach (var data in rows)
    {
        if (data.TryGetValue("details", out object valor))
        {

            JObject[] obj = JsonConvert.DeserializeObject<JObject[]>((string)valor);

            JObject firstData = obj[0];
            JObject secondData = obj[1];

            string isAMCNPortal = "T";
            string mailAddressStreet = (string)firstData["value"]["17"];
            string city = (string)firstData["value"]["14"];
            string postalCode = (string)firstData["value"]["18"];
            string state = (string)firstData["value"]["16"];
            string primaryContactNumber = (string)firstData["value"]["0"];
            string email = (string)firstData["value"]["4"];
            string primaryFirstName = (string)firstData["value"]["2"];
            string primaryLastName = (string)firstData["value"]["1"];
            string primaryDateOfBirth = (string)firstData["value"]["22"];
            string planCodeId = (string)firstData["value"]["20"];
            string getCodeId = (string)firstData["value"]["19"];
            string trackCodeId = (string)firstData["value"]["21"];

            //Console.WriteLine($"{postalCode} {contactNumber} {primaryFirstName} {primaryLastName} {planCodeId}");

            Account signUpRequestBody = new Account()
            {
                IsAMCNPortal = "T",
                address = new Address()
                {
                    HomeAddress = new StandarAddress()
                    {
                        Line1 = mailAddressStreet,
                        Line2 = null,
                        City = city,
                        PostalCode = postalCode,
                        StateProvinceAbbrevation = state,
                        CountryCode = "US"
                    },
                    MailingAddress = new StandarAddress()
                    {
                        Line1 = mailAddressStreet,
                        Line2 = null,
                        City = city,
                        PostalCode = postalCode,
                        StateProvinceAbbrevation = state,
                        CountryCode = "US"
                    },
                    BillingAddress = new StandarAddress()
                    {
                        Line1 = mailAddressStreet,
                        Line2 = null,
                        City = city,
                        PostalCode = postalCode,
                        StateProvinceAbbrevation = state,
                        CountryCode = "US"
                    },
                },
                contactInfo = new ContactInfo()
                {
                    PrimaryContactNumber = primaryContactNumber,
                    Email = email
                },
                membership = new MembershipBundled()
                {
                    EffectiveDate = "09/25/2024",
                    planInfo = new List<PlanInfo>()
                    {
                        new PlanInfo()
                        {
                            OwnerDescription = "0",
                            Term = "",
                            PlanName = "AirEvac Benefit",
                            PlanAmount = 0,
                            CouponCode = "6276",
                            IsActive = false,
                            IsExpired = false
                        }
                    },
                    Members = new List<Member>()
                    {
                        new Member()
                        {
                            MemberId = 0,
                            DateOfBirth = primaryDateOfBirth,
                            FirstName = primaryFirstName,
                            MiddleName = "",
                            LastName = primaryLastName,
                            Gender = "Male",
                            ParticipantType = "Primary"
                        }
                    } 
                },
                slxConstantInfo = new List<SLXConstants>()
                {
                    new SLXConstants()
                    {
                        PlanCodeID = planCodeId,
                        GetCodeId = getCodeId,
                        TrackCodeId = trackCodeId,
                        OwnerDescription = "AirEvac"
                    }
                }
            };

            string jsonString = JsonConvert.SerializeObject(signUpRequestBody, Formatting.Indented);

            var content = new StringContent(jsonString, Encoding.UTF8, "application/json");

            Console.WriteLine(jsonString);

        }
    }

}
catch (Exception ex)
{
    throw new Exception(ex.Message);
}

