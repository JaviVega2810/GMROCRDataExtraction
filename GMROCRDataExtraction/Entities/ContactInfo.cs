using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class ContactInfo
    {
        public string PrimaryContactNumber { get; set; }
        public string AlternateNumber { get; set; }
        public string MobileNumber { get; set; }
        public string OtherPhone1 { get; set; }
        public string Email { get; set; }
    }
}
