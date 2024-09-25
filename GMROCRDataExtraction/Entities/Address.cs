using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class Address
    {
        public StandarAddress HomeAddress { get; set; }
        public StandarAddress MailingAddress { get; set; }
        public StandarAddress BillingAddress { get; set; }
    }
}
