using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GMROCRDataExtraction.Entities
{
    public class MembershipBundled
    {
        public string EffectiveDate { get; set; }
        public List<PlanInfo> planInfo { get; set; }
        public string StoreCategoryId { get; set; }
        public List<Member> Members { get; set; }
        public string Initials { get; set; }
        public string AuthorizationInitials { get; set; }
    }
}
