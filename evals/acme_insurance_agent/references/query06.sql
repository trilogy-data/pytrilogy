select party_identifier, count( policy.policy_number)  as NoOfPolicies
from agreement_party_role
join policy on agreement_party_role.agreement_identifier = policy.policy_identifier
where agreement_party_role.party_role_code = 'PH'
group by party_identifier
