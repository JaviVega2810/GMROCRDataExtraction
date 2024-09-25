using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class SignUp : SignUpRequest
    {
        public bool IsMemberPresent { get; set; }
        public string? AcceptedStateDisclaimer { get; set; }
    }
}
