select policy_number, company_claim_number
from 
    policy 
    inner join policy_coverage_detail on policy.policy_identifier = policy_coverage_detail.policy_identifier
    inner join claim_coverage on claim_coverage.policy_coverage_detail_identifier = policy_coverage_detail.policy_coverage_detail_identifier
    inner join claim on claim.claim_identifier = claim_coverage.claim_identifier
