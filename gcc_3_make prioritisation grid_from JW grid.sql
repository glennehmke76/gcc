-- import grid from JW as tmp_grid - grid is GDA94
DROP TABLE IF EXISTS tmp_range;
CREATE TABLE tmp_range AS
  (SELECT
    id,
    ST_Simplify(geom, 0.05) AS geom
  FROM range
  WHERE
    taxon_id_r = 'u265c'
    AND class = 1
  );
CREATE INDEX idx_tmp_range_geom ON tmp_range USING gist (geom);

DROP TABLE IF EXISTS segbc_gcc_grid CASCADE;
CREATE TABLE segbc_gcc_grid AS
  SELECT
    tmp_grid.*
  FROM tmp_grid
  JOIN tmp_range ON ST_Intersects(tmp_grid.geom, tmp_range.geom)
;

CREATE INDEX idx_segbc_gcc_grid_geom ON segbc_gcc_grid USING gist (geom);
alter table segbc_gcc_grid
    add constraint segbc_gcc_grid_pk
        primary key (id);

-- % public land in grids
  -- using CAPAD 2022 BUT HUGE GEOM INVALIDITY
  DROP TABLE IF EXISTS tmp_capad;
  CREATE TABLE tmp_capad AS
    (SELECT
      area_capad.id,
      area_capad.geom
    FROM area_capad
    JOIN tmp_range ON ST_Intersects(area_capad.geom, tmp_range.geom)
--     WHERE area_capad.geom_valid = true
    );
  CREATE INDEX idx_tmp_capad_geom ON tmp_capad USING gist (geom);

  DELETE FROM tmp_capad
  WHERE ST_IsValid(geom) = false
  ;

--------------
-- add percentage public land to grid table
alter table segbc_gcc_grid
    drop column if exists perc_pub_land;
alter table segbc_gcc_grid
    add perc_pub_land numeric;
-- public land intersection = 1.5 mins
UPDATE segbc_gcc_grid
SET perc_pub_land = sub.perc_pub_land
FROM
  (SELECT
    segbc_gcc_grid.id AS grid_id,
    ST_Area(
    ST_Transform(
      ST_Union(
        ST_Intersection(segbc_gcc_grid.geom, tmp_capad.geom)), 3112)) / 10000 AS perc_pub_land
  FROM segbc_gcc_grid
  JOIN tmp_capad ON ST_Intersects(segbc_gcc_grid.geom, tmp_capad.geom)
--   WHERE
--     tmp_capad.id < 50
  GROUP BY
    segbc_gcc_grid.id
  )sub
WHERE segbc_gcc_grid.id = sub.grid_id
;
--------------

-- add has_allo_casurina record(s) to grid table
alter table segbc_gcc_grid
    drop column if exists has_allo_casurina;
alter table segbc_gcc_grid
    add has_allo_casurina boolean;
UPDATE segbc_gcc_grid
SET has_allo_casurina = true
FROM
  (SELECT
    segbc_gcc_grid.id AS grid_id
  FROM segbc_gcc_grid
  JOIN segbc_gcc_ala_allo_casuarina_records ON ST_Intersects(segbc_gcc_grid.geom, segbc_gcc_ala_allo_casuarina_records.geom)
  )sub
WHERE segbc_gcc_grid.id = sub.grid_id
;

-- add segbc summary records to grid table
alter table segbc_gcc_grid
    drop column if exists latest_segbc_year;
alter table segbc_gcc_grid
    add latest_segbc_year integer;

DROP TABLE IF EXISTS tmp_segbc_gcc_records;
CREATE TABLE tmp_segbc_gcc_records(
  data_source text not null,
  dataset_id integer default null, -- the dataset survey primary key
  year integer default null,
  geom geometry(Point,4283)
);
create index sidx_tmp_segbc_gcc_records_geom on tmp_segbc_gcc_records using gist (geom);

INSERT INTO tmp_segbc_gcc_records (data_source, dataset_id, year, geom)
SELECT
  CONCAT('Coffs Coast third party', '_', data_source),
  id,
  coalesce(extract(year from segbc_habitat_points.date),0) :: integer AS year,
  geom
FROM segbc_habitat_points
WHERE data_source NOT LIKE 'birdata%'
;

INSERT INTO tmp_segbc_gcc_records (data_source, dataset_id, year, geom)
SELECT
  CONCAT('birdata', '_', 'birdata_265_sighting'),
  survey.id,
  extract(year from survey.start_date) :: integer AS year,
  survey_point.geom AS geom
FROM survey
JOIN survey_point ON survey.survey_point_id = survey_point.id
JOIN sighting ON survey.id = sighting.survey_id
WHERE
  sighting.species_id = 265
  AND sighting.individual_count > 0
;

INSERT INTO tmp_segbc_gcc_records (data_source, dataset_id, year, geom)
SELECT
  CONCAT('birdata', '_', 'birdata_265_pseudo_sighting'),
  survey.id,
  extract(year from survey.start_date) :: integer AS year,
  survey_point.geom AS geom
FROM survey
JOIN survey_point ON survey.survey_point_id = survey_point.id
JOIN sighting ON survey.id = sighting.survey_id
JOIN segbc_survey_feed_tree_chewings ON survey.id = segbc_survey_feed_tree_chewings.survey_id
WHERE
  segbc_survey_feed_tree_chewings.chewings_id <= 3
  AND sighting.individual_count IS NULL
;

UPDATE segbc_gcc_grid
SET latest_segbc_year = sub.latest_year
FROM
  (SELECT
    segbc_gcc_grid.id AS grid_id,
    MAX(tmp_segbc_gcc_records.year) AS latest_year
  FROM tmp_segbc_gcc_records
  JOIN segbc_gcc_grid ON ST_Intersects(segbc_gcc_grid.geom, tmp_segbc_gcc_records.geom)
  GROUP BY
    segbc_gcc_grid.id
  )sub
WHERE segbc_gcc_grid.id = sub.grid_id
;

-- select on filters
DROP VIEW IF EXISTS segbc_gcc_priority_grids;
CREATE VIEW segbc_gcc_priority_grids AS
SELECT
  segbc_gcc_grid.id,
  segbc_gcc_grid.has_allo_casurina,
  segbc_gcc_grid.latest_segbc_year,
  segbc_gcc_grid.perc_pub_land,
  segbc_gcc_grid.geom
FROM segbc_gcc_grid
WHERE
  (latest_segbc_year > 2012
  OR latest_segbc_year = 0)
  AND has_allo_casurina = true
  AND perc_pub_land >= 30
;

-- clean-up
DROP TABLE IF EXISTS
  tmp_capad,
  tmp_range,
  tmp_segbc_gcc_records;
;

