--CREATE gen_mnO_index for case alg
--JOIN INDEX CONSTRUCTION--GENERATOR MN ORIGIN
SELECT 
	b.origin, 
	f.gid, 
	b.big_air, 
	f.geom AS f_geom, 
	b.origin_geom AS a_geom
INTO gen_mnO_index
FROM bts_t100_mn_cargo AS b, fda_sites_points AS f
WHERE 
	ST_DWithin(b.origin_geom, f.geom,144841.0)
	AND b.year = 2015 
	AND b.dest_country = 'US' 
	AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD')
GROUP BY b.origin, f.gid, f.freight_ty, b.big_air, f_geom, a_geom;
--CASE 0
WITH case0 as (
	SELECT 
		gid, 
		COUNT(1)
	FROM gen_mnO_index
	GROUP BY gid
	HAVING 
		COUNT(1)=1
)
SELECT 
	c0.gid, 
	gi.origin
INTO gen_mnO_index_FINAL
FROM case0 c0, gen_mnO_index gi
WHERE 
	c0.gid = gi.gid;
--CASE 1
WITH case1 as (
	SELECT 
		gid
		COUNT(1)
	FROM gen_mnO_index
	GROUP BY gid 
	HAVING 
		COUNT(1) > 1
)
INSERT INTO gen_mnO_index_FINAL (gid, origin)
SELECT 
	c1.gid, 
	gi.origin
FROM case1 AS c1, gen_mnO_index AS gi
WHERE 
	c1.gid = gi.gid 
	AND big_air = 1;
--CASE 2
WITH case2 AS (
	SELECT 
		gid, 
		MIN(ST_Distance(f_geom, a_geom)) AS dist,
		COUNT(1)
	FROM gen_mnO_index
	GROUP BY gid 
	HAVING 
		COUNT(1) > 1 
		AND (SUM(big_air) = 0 OR SUM(big_air) = 2)
)
INSERT INTO gen_mnO_index_FINAL (gid, origin)
SELECT c2.gid, gi.origin
FROM case2 c2, gen_mnO_index gi
WHERE c2.gid = gi.gid AND ST_Distance(gi.f_geom, gi.a_geom) = c2.dist;

--JOIN INDEX CONSTRUCTION--HOSP BED COUNT MN ORIGIN
SELECT 
	b.dest, 
	f.gid, 
	b.big_aird, 
	f.beds, 
	f.geom AS f_geom, 
	b.dest_geom AS a_geom
INTO hosp_mnO_index
FROM bts_t100_mn_cargo AS b, hospital_points AS f
WHERE 
	ST_DWithin(b.dest_geom, f.geom,144841.0)
	AND b.year = 2015 
	AND b.dest_country = 'US' 
	AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD')
GROUP BY b.dest, f.gid, f.beds, b.big_aird, f_geom, a_geom;
--CASE 0
WITH case0 AS (
	SELECT 
		gid, 
		COUNT(1)
	FROM hosp_mnO_index
	GROUP BY gid
	HAVING 
	COUNT(1)=1
)
SELECT 
	c0.gid, 
	h.dest, 
	h.beds
INTO hosp_mnO_index_FINAL
FROM case0 AS c0, hosp_mnO_index AS h
WHERE c0.gid = h.gid;

--CASE 1
WITH case1 AS (
	SELECT 
		gid,
		COUNT(1)		
	FROM hosp_mnO_index
	GROUP BY gid 
	HAVING COUNT(1) > 1 
	AND SUM(big_aird) = 1
)

INSERT INTO hosp_mnO_index_FINAL (gid, dest, beds)
SELECT 
	c1.gid, 
	h.dest, 
	h.beds
FROM case1 AS c1, hosp_mnO_index AS h
WHERE 
	c1.gid = gi.gid 
	AND big_aird = 1;

--CASE 2
WITH case2 AS (
	SELECT 
		gid,
		COUNT(1), 
		MIN(ST_Distance(f_geom, a_geom)) as dist
	FROM hosp_mnO_index
	GROUP BY gid 
	HAVING 
	COUNT(1) > 1 
	AND SUM(big_aird) <> 1
)

INSERT INTO hosp_mnO_index_FINAL (gid, dest, beds)
SELECT 
	c2.gid, 
	h.dest, 
	h.beds
FROM case2 AS c2, hosp_mnO_index AS h
WHERE 
	c2.gid = h.gid 
	AND ST_Distance(h.f_geom, h.a_geom) = c2.dist;

--

WITH geom_state AS (
	SELECT name, geom
	FROM faf_states_final
	WHERE name <> 'Rest of MN' AND  name <> 'Minneapolis-St. Paul MN-WI (MN Part)'
),

geom_mn AS (
	SELECT name, geom
	FROM faf_states_final
	WHERE name = 'Minnesota' --name = 'Rest of MN' OR name = 'Minneapolis-St. Paul MN-WI (MN Part)'
),

OD_AVol_mnO AS(
	SELECT b.origin, b.dest, m.name || ' - ' || s.name as OD_id_st, b.origin_geom, b.dest_geom, b.origin || ' - ' || b.dest as OD_id_a, SUM(b.cargo_tons) as volume_tons
	FROM bts_t100_mn_cargo b, geom_mn m, geom_state s
	WHERE origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND year = 2015 AND 
		(ST_Intersects(b.origin_geom, m.geom) AND ST_Intersects(b.dest_geom, s.geom))
	GROUP BY b.origin, b.dest, OD_id_st, b.origin_geom, b.dest_geom, OD_id_a
),

gen_mnO AS (
	SELECT b.origin,b.dest, b.OD_id as OD_id_a, COUNT(DISTINCT g.gid) AS gen_count
	FROM bts_t100_mn_cargo b, gen_mnO_index_FINAL g
	WHERE b.year = 2015 AND b.dest_country = 'US' AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND b.origin = g.origin
	GROUP BY b.origin, b.dest, b.OD_id
),

rec_mnO AS (
	SELECT b.origin,b.dest, b.OD_id as OD_id_a, COUNT(DISTINCT r.gid) AS rec_count
	FROM bts_t100_mn_cargo b, rec_mnO_index_FINAL r
	WHERE b.year = 2015 AND b.dest_country = 'US' AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND b.dest = r.dest
	GROUP BY b.origin, b.dest, b.OD_id
),

hosp_mnO AS (
SELECT b.origin,b.dest, b.OD_id as OD_id_a, SUM(DISTINCT h.beds) AS hosp_bed_count
FROM bts_t100_mn_cargo b, hosp_mnO_index_FINAL h
WHERE b.year = 2015 AND b.dest_country = 'US' AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND b.dest = h.dest
GROUP BY b.origin, b.dest, b.OD_id
)

SELECT a1.origin, a1.dest, a1.OD_id_a, a1.OD_id_st, g.gen_count, r.rec_count, h.hosp_bed_count
FROM OD_AVol_mnO a1, gen_mnO g, rec_mnO r, hosp_mnO h
WHERE a1.OD_id_a = g.OD_id_a AND a1.OD_id_a = r.OD_id_a AND a1.OD_id_a = h.OD_id_a;


--CREATE gen_mnD_index for case alg
--JOIN INDEX CONSTRUCTION--GENERATOR MN DEST
SELECT b.origin, f.gid, b.big_air, f.geom as f_geom, b.origin_geom as a_geom
INTO gen_mnD_index
FROM bts_t100_mn_cargo b, fda_sites_points f
WHERE ST_DWithin(b.origin_geom, f.geom,144841.0)
	AND b.year = 2015 AND b.origin_country = 'US' 
	AND b.dest IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD')
GROUP BY b.origin, f.gid, b.big_air, f_geom, a_geom;

--CASE 0
WITH case0 as (
SELECT gid, count(1)
FROM gen_mnD_index
GROUP BY gid
HAVING count(1)=1
)

SELECT c0.gid, gi.origin
INTO gen_mnD_index_FINAL
FROM case0 c0, gen_mnD_index gi
WHERE c0.gid = gi.gid;

--CASE 1
WITH case1 as (
	SELECT gid, COUNT(1)
	FROM gen_mnD_index
	GROUP BY gid 
	HAVING COUNT(1) > 1 AND SUM (big_air) = 1
)

INSERT INTO gen_mnD_index_FINAL (gid, origin)
SELECT c1.gid, gi.origin
FROM case1 c1, gen_mnD_index gi
WHERE c1.gid = gi.gid AND big_air = 1;

--CASE 2
WITH case2 as (
	SELECT gid,COUNT(gid), MIN(ST_Distance(f_geom, a_geom)) as dist
	FROM gen_mnD_index
	GROUP BY gid 
	HAVING COUNT(gid) > 1 AND (SUM(big_air) = 0 OR SUM(big_air) > 1)
)

INSERT INTO gen_mnD_index_FINAL (gid, origin)
SELECT c2.gid, gi.origin
FROM case2 c2, gen_mnD_index gi
WHERE c2.gid = gi.gid AND ST_Distance(gi.f_geom, gi.a_geom) = c2.dist;


--

--JOIN INDEX CONSTRUCTION--HOSP BED COUNT MN DEST
SELECT b.dest, f.gid, b.big_aird, f.beds, f.geom as f_geom, b.dest_geom as a_geom
INTO hosp_mnD_index
FROM bts_t100_mn_cargo b, hospital_points f
WHERE ST_DWithin(b.dest_geom, f.geom,144841.0)
	AND b.year = 2015 AND b.origin_country = 'US' 
	AND b.dest IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD')
GROUP BY b.dest, f.gid, f.beds, b.big_aird, f_geom, a_geom;

SELECT * FROM hosp_mnD_index
SELECT DISTINCT origin FROM bts_t100_mn_cargo WHERE big_air = 1

--CASE 0
WITH case0 as (
SELECT gid, count(1)
FROM hosp_mnD_index
GROUP BY gid
HAVING count(1)=1
)

SELECT c0.gid, gi.dest, gi.beds
INTO hosp_mnD_index_FINAL
FROM case0 c0, hosp_mnD_index gi
WHERE c0.gid = gi.gid;

--CASE 1
WITH case1 as (
	SELECT gid, COUNT(1) 
	FROM hosp_mnD_index
	GROUP BY gid 
	HAVING COUNT(1) > 1 AND SUM (big_aird) = 1
)

INSERT INTO hosp_mnD_index_FINAL (gid, dest, beds)
SELECT c1.gid, gi.dest, gi.beds
FROM case1 c1, hosp_mnD_index gi
WHERE c1.gid = gi.gid AND big_aird = 1;

--CASE 2
WITH case2 as (
	SELECT gid, COUNT(1), MIN(ST_Distance(f_geom, a_geom)) as dist
	FROM hosp_mnD_index
	GROUP BY gid 
	HAVING COUNT(1) > 1 AND 
	SUM(big_aird) <> 1
)

INSERT INTO hosp_mnD_index_FINAL (gid, dest, beds)
SELECT c2.gid, gi.dest, gi.beds
FROM case2 c2, hosp_mnD_index gi
WHERE c2.gid = gi.gid AND ST_Distance(gi.f_geom, gi.a_geom) = c2.dist;

--Create state geometries
WITH geom_state AS (
	SELECT 
		name, 
		geom
	FROM faf_states_final
	WHERE 
		name <>'Rest of MN' 
		AND  name <> 'Minneapolis-St. Paul MN-WI (MN Part)'
), geom_mn AS (
	SELECT 
		name, 
		geom
	FROM faf_states_final
	WHERE 
		name = 'Rest of MN' 
		OR name = 'Minneapolis-St. Paul MN-WI (MN Part)'
		
--OD Airport-Volume Matrix
	/* 
	CTE 1a: Isolate participating Minnesota airport origins and sum their total trip volume to each national participating destination. Create two IDs (OD_id_st & OD_id_a)
	for operations conducted at the two geographically aggregate areas (state & airport sites respectively).
	
	CTE 1b: Isolate participating Minnesota airport destinations and sum their total trip volume from each national participating origin. Create two IDs (OD_id_st & OD_id_a)
	for operations conducted at the two geographically aggregate areas (state & airport sites respectively).
	*/ 

--Create CTE 1a: OD Airport-Volume Matrix (MN Origin)
),OD_AVol_mnO AS(
	SELECT 
		b.origin, 
		b.dest, 
		m.name || ' - ' || s.name AS OD_id_st, 
		b.origin || ' - ' || b.dest AS OD_id_a,
		b.origin_geom, 
		b.origin_lon,
		b.origin_lat, 
		b.dest_geom, 
		b.dest_lon, 
		b.dest_lat,
		SUM(b.cargo_tons) AS volume_tons
	FROM bts_t100_mn_cargo AS b, geom_mn AS m, geom_state AS s
	WHERE 
		origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') 
		AND year = 2015 
		AND (ST_Intersects(b.origin_geom, m.geom) AND ST_Intersects(b.dest_geom, s.geom))
	GROUP BY b.origin, b.dest, OD_id_st, b.origin_geom, b.origin_lon, b.origin_lat, b.dest_geom, b.dest_lon, b.dest_lat, OD_id_a
	
--OD State-Volume Matrix
	/* 
	CTE 2a: Isolate participating Minnesota state origins and sum their total trip volume to each national participating state destination. Using CTE 1a,
	2a aggregates using GROUP BY OD_id_st.
	
	CTE 2b: Isolate participating Minnesota state dest and sum their total trip volume from each national participating state orig. Using CTE 1b,
	2b aggregates using GROUP BY OD_id_st
	*/ 

--Create CTE 2a: OD State-Volume Matrix (MN Origin)
), OD_Svol_mnO AS(
	SELECT 
		OD_id_st, 
		SUM(volume_tons) AS volume_tons
	FROM OD_AVol_mnO
	GROUP BY OD_id_st

--OD State-Value Matrix
	/*
	Using the faf_zone_mn dataset, these CTEs isolate the summed value per OD state (OD_id_st).
	CTEs tabulate summed value for both Air ONLY and Air + Multiple modes (which includes shipment weights < 100lbs)
	*/ 
	
--Create CTE 3a: OD State-Value Matrix (MN Origin)
--For air (3a1)
), OD_Sval_mnO_air AS(
	SELECT 
		dms_orig || ' - ' || dms_dest AS OD_id_st, 
		SUM(m$_2015) AS value_$m
	FROM faf_zone_mn
	WHERE 
		(sctg2 = 'Pharmaceuticals' OR sctg2 = 'Precision instruments') 
		AND dms_mode = 'Air (include truck-air)' 
		AND (dms_orig = 'Rest of MN' OR dms_orig = 'Minneapolis-St. Paul MN-WI (MN Part)')
	GROUP BY OD_id_st
--For air+multi (3a2)
), OD_Sval_mnO_multi AS (
	SELECT 
		dms_orig || ' - ' || dms_dest AS OD_id_st, 
		SUM(m$_2015) AS value_$m
	FROM faf_zone_mn
	WHERE 
		(sctg2 = 'Pharmaceuticals' OR sctg2 = 'Precision instruments')
		AND (dms_mode = 'Air (include truck-air)' OR dms_mode = 'Multiple modes & mail')
		AND (dms_orig = 'Rest of MN' OR dms_orig = 'Minneapolis-St. Paul MN-WI (MN Part)')
	GROUP BY OD_id_st
--Weights OD Airport Matrix
	/*
	Using spatial tools, create a 'weight matrix' that looks at counts of medical recievers and generators within a 90mi BUFFER of each airport and calibrated on the state-level. 
	Generators influnce origin value (derived from the FDA pharmaceutical and medical device registration list)
	Recievers influence destination value. This count includes the FDA registration list as well hospital locations. Hospitals are additionally weighted using size variables (# of beds)
	A traffic shadow effect is implented:
		CASE 1: IF airportA is WITIHIN 90 miles of airportB AND airportB cargo_tons > 30K then all reciever/generators located in OVERLAPPING buffers are assigned to airportB
		CASE 2: ELSE all reciever generators located in OVERLAPPING buffer go to whichever airport is CLOSEST
	*/ 
	
--Create CTE 5a: Weights Airport Matrix (MN origin)	
--Generators
), gen_mnO AS (
	SELECT 
		b.origin,
		b.dest, 
		b.OD_id AS OD_id_a, 
		COUNT(DISTINCT g.gid) AS gen_count
	FROM bts_t100_mn_cargo AS b, gen_mnO_index_FINAL AS g
	WHERE 
		b.year = 2015 
		AND b.dest_country = 'US' 
		AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') 
		AND b.origin = g.origin
	GROUP BY b.origin, b.dest, b.OD_id
--Hospitals
), hosp_mnO AS (
	SELECT 
		b.origin,
		b.dest, 
		b.OD_id AS OD_id_a, 
		SUM(DISTINCT h.beds) AS hosp_bed_count
	FROM bts_t100_mn_cargo AS b, hosp_mnO_index_FINAL AS h
	WHERE 
		b.year = 2015 
		AND b.dest_country = 'US' 
		AND b.origin IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') 
		AND b.dest = h.dest
	GROUP BY b.origin, b.dest, b.OD_id
--Normalize Weight Matrix
), weights_states AS (
	SELECT 
		a1.OD_id_st, 
		SUM(a1.volume_tons) AS volume_tons, 
		SUM(g.gen_count) AS gen_count, 
		SUM(h.hosp_bed_count) AS hosp_bed_count
	FROM OD_AVol_mnO AS a1, gen_mnO AS g, hosp_mnO AS h
	WHERE 
		a1.OD_id_a = g.OD_id_a 
		AND a1.OD_id_a = h.OD_id_a
	GROUP BY a1.OD_id_st
), weights_airports_normalize AS (
	SELECT 
		a1.origin, 
		a1.dest, a1.OD_id_a, 
		a1.OD_id_st,
		(((a1.volume_tons*100)/s.volume_tons)*(g.gen_count*100)/s.gen_count)*((h.hosp_bed_count*100)/s.hosp_bed_count) AS combined_weight
	FROM OD_AVol_mnO AS a1, weights_states AS s, gen_mnO AS g, hosp_mnO AS h
	WHERE 
		a1.OD_id_st = s.OD_id_st 
		AND a1.OD_id_a = g.OD_id_a 
		AND a1.OD_id_a = h.OD_id_a
), weights_states_CombinedWeights AS (
	SELECT 
	n.OD_id_st, 
	SUM(n.combined_weight) AS combined_weight
	FROM weights_airports_normalize AS n
	GROUP BY n.OD_id_st
)

--RUN ORIGIN; Create Table Statement
SELECT 
	a1.origin, 
	a1.dest, 
	a1.OD_id_a, 
	a1.volume_tons, 
	a1.OD_id_st, 
	a1.origin_geom, 
	a1.origin_lon, 
	a1.origin_lat, 
	a1.dest_geom, 
	a1.dest_lon, 
	a1.dest_lat,
	g.gen_count, 
	h.hosp_bed_count,
	(n.combined_weight/c.combined_weight)* a31.value_$m AS value_$m_air_w,
	(n.combined_weight/c.combined_weight)* a32.value_$m AS value_$m_multi_w
INTO MN_Origin_CargoValue_2
FROM OD_AVol_mnO AS a1, OD_Sval_mnO_air AS a31, OD_Sval_mnO_multi AS a32, gen_mnO AS g,hosp_mnO AS h, weights_airports_normalize AS n, weights_states_CombinedWeights AS c
WHERE 
	a1.OD_id_st = a31.OD_id_st 
	AND a1.OD_id_st = a32.OD_id_st 
	AND a1.OD_id_st = c.OD_id_st 
	AND a1.OD_id_a = n.OD_id_a 
	AND a1.OD_id_a = g.OD_id_a 
	AND a1.OD_id_a = h.OD_id_a
ORDER BY value_$m_air_w DESC;

--Create state geometries
WITH geom_state AS (
	SELECT name, geom
	FROM faf_states_final
	WHERE name <> 'Rest of MN' AND  name <> 'Minneapolis-St. Paul MN-WI (MN Part)'
),

geom_mn AS (
	SELECT name, geom
	FROM faf_states_final
	WHERE name = 'Rest of MN' OR name = 'Minneapolis-St. Paul MN-WI (MN Part)'
),

--OD Airport-Volume Matrix
	/* 
	CTE 1a: Isolate participating Minnesota airport origins and sum their total trip volume to each national participating destination. Create two IDs (OD_id_st & OD_id_a)
	for operations conducted at the two geographically aggregate areas (state & airport sites respectively).
	
	CTE 1b: Isolate participating Minnesota airport destinations and sum their total trip volume from each national participating origin. Create two IDs (OD_id_st & OD_id_a)
	for operations conducted at the two geographically aggregate areas (state & airport sites respectively).
	*/ 
	
--Create CTE 1b: OD Airport-Volume Matrix (MN Dest)
OD_AVol_mnD AS(
	SELECT b.origin, b.dest, s.name || ' - ' || m.name as OD_id_st, b.origin || ' - ' || b.dest as OD_id_a, 
	b.origin_geom, b.origin_lon, b.origin_lat, b.dest_geom, b.dest_lon, b.dest_lat, 
	SUM(b.cargo_tons) as volume_tons
	FROM bts_t100_mn_cargo b, geom_mn m, geom_state s
	WHERE dest IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND year = 2015 AND 
		(ST_Intersects(b.dest_geom, m.geom) AND ST_Intersects(b.origin_geom, s.geom))
	GROUP BY b.origin, b.dest, OD_id_st, b.origin_geom, b.dest_geom, OD_id_a,b.origin_lon, b.origin_lat,b.dest_lon, b.dest_lat  
),

--OD State-Volume Matrix
	/* 
	CTE 2a: Isolate participating Minnesota state origins and sum their total trip volume to each national participating state destination. Using CTE 1a,
	2a aggregates using GROUP BY OD_id_st.
	
	CTE 2b: Isolate participating Minnesota state dest and sum their total trip volume from each national participating state orig. Using CTE 1b,
	2b aggregates using GROUP BY OD_id_st
	*/ 
--Create CTE 2b: OD State-Volume Matrix (MN Dest)
OD_Svol_mnD AS(
	SELECT OD_id_st, SUM(volume_tons) as volume_tons
	FROM OD_AVol_mnD
	GROUP BY OD_id_st
),

--OD State-Value Matrix
	/*
	Using the faf_zone_mn dataset, these CTEs isolate the summed value per OD state (OD_id_st).
	CTEs tabulate summed value for both Air ONLY and Air + Multiple modes (which includes shipment weights < 100lbs)
	*/ 
	
--Create CTE 3b: OD State-Value Matrix (MN Origin)

--For air (3b1)
OD_Sval_mnD_air AS(
	SELECT dms_orig || ' - ' || dms_dest AS OD_id_st, SUM(m$_2015) as value_$m
	FROM faf_zone_mn
	WHERE (sctg2 = 'Pharmaceuticals' OR sctg2 = 'Precision instruments')
	AND dms_mode = 'Air (include truck-air)'
	AND (dms_dest = 'Rest of MN' OR dms_dest = 'Minneapolis-St. Paul MN-WI (MN Part)') 
	GROUP BY OD_id_st
),
--For air+multi (3b2)
OD_Sval_mnD_multi AS (
	SELECT dms_orig || ' - ' || dms_dest AS OD_id_st, SUM(m$_2015) as value_$m
	FROM faf_zone_mn
	WHERE (sctg2 = 'Pharmaceuticals' OR sctg2 = 'Precision instruments')
	AND (dms_mode = 'Air (include truck-air)' OR dms_mode = 'Multiple modes & mail')
	AND (dms_dest = 'Rest of MN' OR dms_dest = 'Minneapolis-St. Paul MN-WI (MN Part)') 
	GROUP BY OD_id_st
),

--Weights OD Airport Matrix
	/*
	Using spatial tools, create a 'weight matrix' that looks at counts of medical recievers and generators within a 90mi BUFFER of each airport and calibrated on the state-level. 
	Generators influnce origin value (derived from the FDA pharmaceutical and medical device registration list)
	Recievers influence destination value. This count includes the FDA registration list as well hospital locations. Hospitals are additionally weighted using size variables (# of beds)
	A traffic shadow effect is implented:
		CASE 1: IF airportA is WITIHIN 90 miles of airportB AND airportB cargo_tons > 30K then all reciever/generators located in OVERLAPPING buffers are assigned to airportB
		CASE 2: ELSE all reciever generators located in OVERLAPPING buffer go to whichever airport is CLOSEST
	*/ 
	
--Create CTE 5b: Weights Airport Matrix (MN Dest)	
gen_mnD AS (
	SELECT b.origin,b.dest, b.OD_id as OD_id_a, COUNT(DISTINCT g.gid) AS gen_count
	FROM bts_t100_mn_cargo b, gen_mnD_index_FINAL g
	WHERE b.year = 2015 AND b.origin_country = 'US' AND b.dest IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND b.origin = g.origin
	GROUP BY b.origin, b.dest, b.OD_id
),

hosp_mnD AS (
	SELECT b.origin,b.dest, b.OD_id as OD_id_a, SUM(DISTINCT h.beds) AS hosp_bed_count
	FROM bts_t100_mn_cargo b, hosp_mnD_index_FINAL h
	WHERE b.year = 2015 AND b.origin_country = 'US' AND b.dest IN ('RST', 'MSP', 'DLH', 'TVF', 'BJI', 'BRD') AND b.dest = h.dest
	GROUP BY b.origin, b.dest, b.OD_id
),

--Normalize Weight Matrix
weights_states AS (
	SELECT b1.OD_id_st, SUM(b1.volume_tons) as volume_tons, SUM(g.gen_count) as gen_count, SUM(h.hosp_bed_count) as hosp_bed_count 
	FROM OD_AVol_mnD b1, gen_mnD g, hosp_mnD h
	WHERE b1.OD_id_a = g.OD_id_a AND b1.OD_id_a = h.OD_id_a
	GROUP BY b1.OD_id_st
),

weights_airports_normalize AS (
	SELECT b1.origin, b1.dest, b1.OD_id_a, b1.OD_id_st,
		(((b1.volume_tons*100)/s.volume_tons)*(g.gen_count*100)/s.gen_count)*((h.hosp_bed_count*100)/s.hosp_bed_count) as combined_weight
	FROM OD_AVol_mnD b1, weights_states s, gen_mnD g, hosp_mnD h
	WHERE b1.OD_id_st = s.OD_id_st AND b1.OD_id_a = g.OD_id_a AND b1.OD_id_a = h.OD_id_a
),

weights_states_CombinedWeights AS (
	SELECT n.OD_id_st, SUM(n.combined_weight) as combined_weight
	FROM weights_airports_normalize n
	GROUP BY n.OD_id_st
)

--RUN DEST
SELECT b1.origin, b1.dest, b1.OD_id_a, b1.volume_tons, b1.OD_id_st,
g.gen_count, h.hosp_bed_count, b1.origin_lon, b1.origin_lat,b1.dest_lon, b1.dest_lat, b1.origin_geom, b1.dest_geom,
(n.combined_weight/c.combined_weight)* b31.value_$m as value_$m_air_w,
(n.combined_weight/c.combined_weight)* b32.value_$m as value_$m_multi_w
INTO MN_Dest_CargoValue_2
FROM OD_AVol_mnD b1, OD_Sval_mnD_air b31, OD_Sval_mnD_multi b32, gen_mnD g, hosp_mnD h, weights_airports_normalize n, weights_states_CombinedWeights c
WHERE b1.OD_id_st = b31.OD_id_st AND
	b1.OD_id_st = b32.OD_id_st AND 
	b1.OD_id_st = c.OD_id_st AND 
	b1.OD_id_a = n.OD_id_a AND 
	b1.OD_id_a = g.OD_id_a AND 
	b1.OD_id_a = h.OD_id_a
ORDER BY value_$m_air_w DESC;


