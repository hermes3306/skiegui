// Delete all nodes
MATCH (n) DETACH DELETE n;

// Load Apartment data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Apartment.json") YIELD value AS records1
UNWIND records1 AS apartment
CREATE (a:Apartment)
WITH a, apartment.n.properties as apartmentProperties
SET a = apartmentProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(apartmentProperties.coord.y), longitude: toFloat(apartmentProperties.coord.x)});

// Load Academy data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Academy.json") YIELD value AS records2
UNWIND records2 AS record
CREATE (a:Academy)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load ApartmentType data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/ApartmentType.json") YIELD value AS records3
UNWIND records3 AS record
CREATE (a:ApartmentType)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load BusStation data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/BusStation.json") YIELD value AS records4
UNWIND records4 AS record
CREATE (a:BusStation)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Contract data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Contract.json") YIELD value AS records5
UNWIND records5 AS record
CREATE (a:Contract)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Convention data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Convention.json") YIELD value AS records6
UNWIND records6 AS record
CREATE (a:Convention)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Daycare data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Daycare.json") YIELD value AS records7
UNWIND records7 AS record
CREATE (a:Daycare)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Departmentstore data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Departmentstore.json") YIELD value AS records8
UNWIND records8 AS record
CREATE (a:Departmentstore)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load DistrictBoundaryDong data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/DistrictBoundaryDong.json") YIELD value AS records9
UNWIND records9 AS record
CREATE (a:DistrictBoundaryDong)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load DistrictBoundaryGu data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/DistrictBoundaryGu.json") YIELD value AS records10
UNWIND records10 AS record
CREATE (a:DistrictBoundaryGu)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load GoodWayToWalk data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/GoodWayToWalk.json") YIELD value AS records11
UNWIND records11 AS record
CREATE (a:GoodWayToWalk)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Highway data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Highway.json") YIELD value AS records12
UNWIND records12 AS record
CREATE (a:Highway)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Hospital data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Hospital.json") YIELD value AS records13
UNWIND records13 AS record
CREATE (a:Hospital)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Kinder data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Kinder.json") YIELD value AS records14
UNWIND records14 AS record
CREATE (a:Kinder)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Mart data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Mart.json") YIELD value AS records15
UNWIND records15 AS record
CREATE (a:Mart)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Park data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Park.json") YIELD value AS records16
UNWIND records16 AS record
CREATE (a:Park)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load School data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/School.json") YIELD value AS records17
UNWIND records17 AS record
CREATE (a:School)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Subway data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Subway.json") YIELD value AS records18
UNWIND records18 AS record
CREATE (a:Subway)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load SubwayFuture data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/SubwayFuture.json") YIELD value AS records19
UNWIND records19 AS record
CREATE (a:SubwayFuture)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Theater data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Theater.json") YIELD value AS records20
UNWIND records20 AS record
CREATE (a:Theater)
WITH a, record.n.properties as myProperties
SET a = myProperties {.*, coord: null}
SET a.coord = point({latitude: toFloat(myProperties.coord.y), longitude: toFloat(myProperties.coord.x)});

// Load Relations_IN data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Relations_IN.json") YIELD value AS records21
UNWIND records21 AS record
CREATE (a:Relations_IN)
SET a.src = record.`n.uuid`
SET a.tar = record.`d.uuid`;

// Create IN relationships
MATCH (n), (r:Relations_IN), (d:DistrictBoundaryDong) 
WHERE n.uuid = r.src AND d.uuid = r.tar 
CREATE (n)-[:IN]->(d);

// Load Relations_HAS data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Relations_HAS.json") YIELD value AS has_records
UNWIND has_records AS record
CREATE (a:Relations_HAS)
SET a.src = record.auuid
SET a.tar = record.buuid;

// Create HAS_TYPE relationships
MATCH (n:Apartment), (r:Relations_HAS), (d:ApartmentType) 
WHERE n.uuid = r.src AND d.uuid = r.tar 
CREATE (n)-[:HAS_TYPE]->(d);

// Load Relations_TRADE data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/Relations_TRADE.json") YIELD value AS trade_records
UNWIND trade_records AS record
CREATE (a:Relations_TRADE)
SET a.src = record.auuid
SET a.tar = record.buuid;

// Create TRADE relationships
MATCH (n:ApartmentType), (r:Relations_TRADE), (d:Contract)
WHERE n.uuid = r.src AND d.uuid = r.tar
CREATE (n)-[:TRADE]->(d);

// Load Reputation data
CALL apoc.load.json("https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/bt_apt_theme.json") YIELD value AS reputation_records
WITH reputation_records
UNWIND reputation_records AS reputation
CREATE (r:Reputation {apartment_uuid: reputation.id})
SET r.uuid = "reputation_" + id(r), r += reputation {.*, id: null};

// Update Reputation UUIDs
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/reputation.csv' AS reputation
MATCH (r:Reputation {apartment_uuid: reputation.apartment_uuid})
SET r.uuid = reputation.uuid;

// Create EVALUATE relationships
MATCH (n:Apartment), (r:Reputation) 
WHERE n.uuid = r.apartment_uuid
CREATE (n)-[:EVALUATE]->(r);

// Load SpecialPurposeArea data
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/json/specialpurposearea.csv' AS spa
CREATE (s:SpecialPurposeArea)
SET s += spa {.*, id: null};


// Load districtboundarydong_with_specialpurposearea data
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/hermes3306/skie/main/skie_home/FINAL_of_Level1/districtboundarydong_with_specialpurposearea.csv' AS dws
CREATE (s:districtboundarydong_with_specialpurposearea) 
SET s += dws {.*, id: null};

// Create new relationships between SpecialPurposeArea and DistrictBoundaryDong
MATCH (n:districtboundarydong_with_specialpurposearea), (s:SpecialPurposeArea), (d:DistrictBoundaryDong)
WHERE (n.uuid_specialpurposearea = s.uuid)
AND (n.uuid_districtboundarydong = d.uuid)
CREATE (s)-[:IN]->(d)

