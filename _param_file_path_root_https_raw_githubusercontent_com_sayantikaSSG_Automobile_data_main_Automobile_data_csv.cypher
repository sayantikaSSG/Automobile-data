:param file_path_root => 'https://raw.githubusercontent.com/sayantikaSSG/Automobile-data/main/Automobile_data.csv';
:param idsToSkip => [];
 
// CONSTRAINT creation
// -------------------
CREATE CONSTRAINT price_price_uniq IF NOT EXISTS
FOR (n:price)
REQUIRE (n.price) IS UNIQUE;
CREATE CONSTRAINT make_make_uniq IF NOT EXISTS
FOR (n:make)
REQUIRE (n.make) IS UNIQUE;
CREATE CONSTRAINT engine_type_engine_type_uniq IF NOT EXISTS
FOR (n:engine_type)
REQUIRE (n.engine_type) IS UNIQUE;
CREATE CONSTRAINT engine_size_engine_size_uniq IF NOT EXISTS
FOR (n:engine_size)
REQUIRE (n.engine_size) IS UNIQUE;
CREATE CONSTRAINT fuel_type_fuel_type_uniq IF NOT EXISTS
FOR (n:fuel_type)
REQUIRE (n.fuel_type) IS UNIQUE;
CREATE CONSTRAINT horsepower_horsepower_uniq IF NOT EXISTS
FOR (n:horsepower)
REQUIRE (n.horsepower) IS UNIQUE;
 
// NODE load
// ---------
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.price IN $idsToSkip AND NOT row.price IS NULL
MERGE (n:price { price: row.price })
SET n.price = row.price;
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.make IN $idsToSkip AND NOT row.make IS NULL
MERGE (n:make { make: row.make })
SET n.make = row.make;
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.engine_type IN $idsToSkip AND NOT row.engine_type IS NULL
MERGE (n:engine_type { engine_type: row.engine_type })
SET n.engine_type = row.engine_type;
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.engine_size IN $idsToSkip AND NOT toInteger(trim(row.engine_size)) IS NULL
MERGE (n:engine_size { engine_size: toInteger(trim(row.engine_size)) })
SET n.engine_size = toInteger(trim(row.engine_size));
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.fuel_type IN $idsToSkip AND NOT row.fuel_type IS NULL
MERGE (n:fuel_type { fuel_type: row.fuel_type })
SET n.fuel_type = row.fuel_type;
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row
WHERE NOT row.horsepower IN $idsToSkip AND NOT row.horsepower IS NULL
MERGE (n:horsepower { horsepower: row.horsepower })
SET n.horsepower = row.horsepower;
 
// RELATIONSHIP load
// -----------------
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:horsepower { horsepower: row.horsepower })
MATCH (target:price { price: row.price })
MERGE (source)-[r:d]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:fuel_type { fuel_type: row.fuel_type })
MATCH (target:price { price: row.price })
MERGE (source)-[r:e]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:make { make: row.make })
MATCH (target:price { price: row.price })
MERGE (source)-[r:a]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:engine_size { engine_size: toInteger(trim(row.engine_size)) })
MATCH (target:price { price: row.price })
MERGE (source)-[r:b]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:engine_type { engine_type: row.engine_type })
MATCH (target:price { price: row.price })
MERGE (source)-[r:c]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:make { make: row.make })
MATCH (target:fuel_type { fuel_type: row.fuel_type })
MERGE (source)-[r:f]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:make { make: row.make })
MATCH (target:horsepower { horsepower: row.horsepower })
MERGE (source)-[r:g]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:make { make: row.make })
MATCH (target:engine_size { engine_size: toInteger(trim(row.engine_size)) })
MERGE (source)-[r:h]->(target);
 
LOAD CSV WITH HEADERS FROM $file_path_root AS row
WITH row 
MATCH (source:make { make: row.make })
MATCH (target:engine_type { engine_type: row.engine_type })
MERGE (source)-[r:i]->(target);