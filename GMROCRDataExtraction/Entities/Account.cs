namespace GMROCRDataExtraction.Entities
{
    public class Account : SignUp
    {
        public string IsAMCNPortal { get; set; }
        public string? MembershipNumber { get; set; }
        public string? AssociatedMembershipNumber { get; set; }
        public string? TestFlags { get; set; }
    }
}
