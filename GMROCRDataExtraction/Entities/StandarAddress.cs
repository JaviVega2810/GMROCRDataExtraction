using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class StandarAddress
    {
        public string Line1 { get; set; }
        public string Line2 { get; set; }
        public string City { get; set; }
        public string PostalCode { get; set; }
        public string StateProvinceAbbrevation { get; set; }

        [DefaultValue("US")]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public string CountryCode { get; set; }
    }
}
