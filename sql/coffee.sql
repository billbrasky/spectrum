CREATE TABLE "beans" (
  "id" int PRIMARY KEY,
  "bag_weight" varchar,
  "number_of_bags" int,
  "species" varchar,
  "variety" varchar,
  "expiration" varchar,
  "grading_date" varchar,
  "harvest_year" int,
  "cupping_id" int,
  "green_id" int,
  "farm_id" int
);

CREATE TABLE "review_cupping" (
  "id" int PRIMARY KEY,
  "acidity" varchar,
  "after_taste" varchar,
  "aroma" varchar,
  "balance" varchar,
  "body" varchar,
  "cleanliness" varchar,
  "cupper_points" varchar,
  "flavor" varchar,
  "sweetness" varchar,
  "total_cup_points" varchar,
  "uniformity" varchar
);

CREATE TABLE "review_green" (
  "id" int PRIMARY KEY,
  "defects_1" varchar,
  "defects_2" varchar,
  "color" varchar,
  "moisture" varchar,
  "quakers" varchar,
  "processing_method" varchar
);

CREATE TABLE "farm" (
  "id" int PRIMARY KEY,
  "owner" varchar,
  "owner_1" varchar,
  "company" varchar,
  "country" varchar,
  "cert_id" int,
  "name" varchar,
  "ico" int,
  "country_partner" varchar,
  "lot_number" int,
  "mill" varchar,
  "region" varchar,
  "producer" varchar,
  "altitude_meters_id" int,
  "altitude_id" int
);

CREATE TABLE "certificate" (
  "id" int PRIMARY KEY,
  "address" varchar,
  "body" varchar,
  "contact" varchar
);

CREATE TABLE "altitude_meters" (
  "id" int PRIMARY KEY,
  "high_meters" varchar,
  "low_meters" varchar,
  "mean_meters" varchar
);

CREATE TABLE "altitude" (
  "id" int PRIMARY KEY,
  "altitude" varchar,
  "unit" char
);

ALTER TABLE "beans" ADD FOREIGN KEY ("cupping_id") REFERENCES "review_cupping" ("id");

ALTER TABLE "beans" ADD FOREIGN KEY ("green_id") REFERENCES "review_green" ("id");

ALTER TABLE "beans" ADD FOREIGN KEY ("farm_id") REFERENCES "farm" ("id");

ALTER TABLE "farm" ADD FOREIGN KEY ("cert_id") REFERENCES "certificate" ("id");

ALTER TABLE "farm" ADD FOREIGN KEY ("altitude_meters_id") REFERENCES "altitude_meters" ("id");

ALTER TABLE "farm" ADD FOREIGN KEY ("altitude_id") REFERENCES "altitude" ("id");
