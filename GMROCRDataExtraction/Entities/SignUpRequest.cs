using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class SignUpRequest
    {
        public string VendorKey { get; set; }
        public string IsAMCNPortal { get; set; }
        public string AccountIdToCreate { get; set; }
        public string HasEvent { get; set; }
        public string GeoLocationState { get; set; }
        public ContactInfo contactInfo { get; set; }
        public MembershipBundled membership { get; set; }
        public Address address { get; set; }
        public List<SLXConstants> slxConstantInfo { get; set; }
        //public PaymentRequest PaymentRequest { get; set; }
    }
}
