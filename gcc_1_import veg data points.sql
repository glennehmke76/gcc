-- ingest provided veg points
-- reduce fields from original files and import both into single integrated table
create table segbc_gcc_ala_allo_casuarina_records (
  "dataResourceUid" text,
  "decimalLatitude" double precision,
  "decimalLongitude" double precision,
  "geodeticDatum" text,
  "coordinateUncertaintyInMeters" double precision,
  "coordinatePrecision" double precision,
  "vernacularName" text,
  species text,
  "recordID" text
);

copy segbc_gcc_ala_allo_casuarina_records FROM '/Users/glennehmke/MEGA/segbc/gcc_priority_areas/Habitat data/ala_allocasuarina_records-2023-05-22/ala_allocasuarina_records_import.csv'  DELIMITER ',' CSV HEADER;
copy segbc_gcc_ala_allo_casuarina_records FROM '/Users/glennehmke/MEGA/segbc/gcc_priority_areas/Habitat data/ala_casuarina_records-2023-05-22/ala_casuarina_records_import.csv'  DELIMITER ',' CSV HEADER;

DELETE FROM
segbc_gcc_ala_allo_casuarina_records
WHERE
  segbc_gcc_ala_allo_casuarina_records."decimalLatitude" IS NULL
  OR segbc_gcc_ala_allo_casuarina_records."decimalLatitude" IS NULL
;

ALTER TABLE IF EXISTS segbc_gcc_ala_allo_casuarina_records
  ADD COLUMN geom geometry(Point,4283);
UPDATE segbc_gcc_ala_allo_casuarina_records
SET geom = ST_SetSRID(ST_MakePoint(segbc_gcc_ala_allo_casuarina_records."decimalLongitude", segbc_gcc_ala_allo_casuarina_records."decimalLatitude"), 4283);
CREATE INDEX idx_segbc_gcc_ala_allocasuarina_records_geom ON segbc_gcc_ala_allo_casuarina_records USING gist (geom);
alter table segbc_gcc_ala_allo_casuarina_records
  add constraint segbc_gcc_ala_allocasuarina_records_pk
    primary key ("recordID");

-- delete records outside range
DELETE FROM segbc_gcc_ala_allo_casuarina_records
USING tmp_range
WHERE
 ST_Disjoint(ST_transform(segbc_gcc_ala_allo_casuarina_records.geom, 3112), tmp_range.geom)
;