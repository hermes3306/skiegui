// Create IN relationships
MATCH (n), (r:Relations_IN), (d:DistrictBoundaryDong) 
WHERE n.uuid = r.src AND d.uuid = r.tar 
CREATE (n)-[:IN]->(d);

// Create HAS_TYPE relationships
MATCH (n:Apartment), (r:Relations_HAS), (d:ApartmentType) 
WHERE n.uuid = r.src AND d.uuid = r.tar 
CREATE (n)-[:HAS_TYPE]->(d);

// Create TRADE relationships
MATCH (n:ApartmentType), (r:Relations_TRADE), (d:Contract)
WHERE n.uuid = r.src AND d.uuid = r.tar
CREATE (n)-[:TRADE]->(d);

// Create EVALUATE relationships
MATCH (n:Apartment), (r:Reputation) 
WHERE n.uuid = r.apartment_uuid
CREATE (n)-[:EVALUATE]->(r);

// Create new relationships between SpecialPurposeArea and DistrictBoundaryDong
MATCH (n:districtboundarydong_with_specialpurposearea), (s:SpecialPurposeArea), (d:DistrictBoundaryDong)
WHERE (n.uuid_specialpurposearea = s.uuid)
AND (n.uuid_districtboundarydong = d.uuid)
CREATE (s)-[:IN]->(d);