using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class Member
    {
            public int MemberId { get; set; }
            public string SlxMemberId { get; set; }
            public string Action { get; set; }
            public string DateOfBirth { get; set; }
            public string FirstName { get; set; }
            public string MiddleName { get; set; }
            public string LastName { get; set; }
            public string Gender { get; set; }

            [DefaultValue("Other")]
            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
            public string ParticipantType { get; set; }
            public string Status { get; set; }
        }
}
