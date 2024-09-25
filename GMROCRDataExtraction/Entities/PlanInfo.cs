using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class PlanInfo
    {
        public string OwnerDescription { get; set; }
        public string Term { get; set; }
        public string PlanName { get; set; }
        public string PlanType { get; set; }
        public decimal PlanAmount { get; set; }
        public string CouponCode { get; set; }
        [JsonIgnore]
        public string SeccodeId { get; set; }
        [JsonIgnore]
        public string OriginalOwnerDescription { get; set; }
        public bool IsActive { get; set; }
        public string ExpirationDate { get; set; }
        public bool IsExpired { get; set; }
    }
}
