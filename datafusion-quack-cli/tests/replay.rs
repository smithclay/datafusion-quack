//! Per-client SQL replay (gate G3).
//!
//! Every statement each client sent in a recorded session, replayed against an
//! in-process server: each must plan and execute. The lists were recorded with
//! `tests-integration/record_proxy.py` in front of `datafusion-quack`:
//!
//! - `DUCKDB_ATTACH_QUERIES`: DuckDB 2.0's `ATTACH` running
//!   `tests-integration/differential.py` (TPC-H SF0.01 and the type matrix);
//! - `PROVIDER_QUERIES`: the `datafusion-table-providers` Quack suite in seeded mode;
//! - `CLIENT_QUERIES`: the read-only subset of the `quack_protocol` live suite.
//!
//! To refresh a list, record a session and copy the PREPARE statements here.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_quack::{QuackServer, ServerOptions};
use datafusion_quack_cli::seed::{Seed, load};
use futures::TryStreamExt;
use quack_protocol::{QuackClient, QuackClientOptions};

/// What DuckDB 2.0's `ATTACH` sent.
const DUCKDB_ATTACH_QUERIES: &[&str] = &[
    r#"
WITH RECURSIVE schema_tree AS (
	SELECT oid, database_name, [schema_name] AS schema_path
	FROM duckdb_schemas()
	WHERE parent_schema_oid IS NULL
	UNION ALL
	SELECT nested.oid, nested.database_name, list_append(parent.schema_path, nested.schema_name)
	FROM duckdb_schemas() nested
	JOIN schema_tree parent ON nested.parent_schema_oid = parent.oid
)
SELECT oid, database_name, schema_path, current_database()
FROM schema_tree
WHERE database_name NOT IN ('system', 'temp')
ORDER BY (database_name = current_database()) DESC, database_name, length(schema_path), schema_path
	"#,
    r#"
SELECT schema_oid, sql, 'table'
FROM duckdb_tables()
UNION ALL
SELECT schema_oid, view_name, 'view'
FROM duckdb_views()
	"#,
    r#"BEGIN TRANSACTION"#,
    r#"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum((l_extendedprice * (1 - l_discount))) AS sum_disc_price, sum(((l_extendedprice * (1 - l_discount)) * (1 + l_tax))) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count_star() AS count_order FROM lineitem WHERE (l_shipdate <= CAST('1998-09-02' AS DATE)) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"#,
    r#"COMMIT"#,
    r#"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part , supplier , partsupp , nation , region WHERE ((p_partkey = ps_partkey) AND (s_suppkey = ps_suppkey) AND (p_size = 15) AND (p_type ~~ '%BRASS') AND (s_nationkey = n_nationkey) AND (n_regionkey = r_regionkey) AND (r_name = 'EUROPE') AND (ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp , supplier , nation , region WHERE ((p_partkey = ps_partkey) AND (s_suppkey = ps_suppkey) AND (s_nationkey = n_nationkey) AND (n_regionkey = r_regionkey) AND (r_name = 'EUROPE'))))) ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"#,
    r#"SELECT l_orderkey, sum((l_extendedprice * (1 - l_discount))) AS revenue, o_orderdate, o_shippriority FROM customer , orders , lineitem WHERE ((c_mktsegment = 'BUILDING') AND (c_custkey = o_custkey) AND (l_orderkey = o_orderkey) AND (o_orderdate < CAST('1995-03-15' AS DATE)) AND (l_shipdate > CAST('1995-03-15' AS DATE))) GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"#,
    r#"SELECT o_orderpriority, count_star() AS order_count FROM orders WHERE ((o_orderdate >= CAST('1993-07-01' AS DATE)) AND (o_orderdate < CAST('1993-10-01' AS DATE)) AND EXISTS(SELECT * FROM lineitem WHERE ((l_orderkey = o_orderkey) AND (l_commitdate < l_receiptdate)))) GROUP BY o_orderpriority ORDER BY o_orderpriority"#,
    r#"SELECT n_name, sum((l_extendedprice * (1 - l_discount))) AS revenue FROM customer , orders , lineitem , supplier , nation , region WHERE ((c_custkey = o_custkey) AND (l_orderkey = o_orderkey) AND (l_suppkey = s_suppkey) AND (c_nationkey = s_nationkey) AND (s_nationkey = n_nationkey) AND (n_regionkey = r_regionkey) AND (r_name = 'ASIA') AND (o_orderdate >= CAST('1994-01-01' AS DATE)) AND (o_orderdate < CAST('1995-01-01' AS DATE))) GROUP BY n_name ORDER BY revenue DESC"#,
    r#"SELECT sum((l_extendedprice * l_discount)) AS revenue FROM lineitem WHERE ((l_shipdate >= CAST('1994-01-01' AS DATE)) AND (l_shipdate < CAST('1995-01-01' AS DATE)) AND (l_discount BETWEEN 0.05 AND 0.07) AND (l_quantity < 24))"#,
    r#"SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, date_part('YEAR', l_shipdate) AS l_year, (l_extendedprice * (1 - l_discount)) AS volume FROM supplier , lineitem , orders , customer , nation AS n1 , nation AS n2 WHERE ((s_suppkey = l_suppkey) AND (o_orderkey = l_orderkey) AND (c_custkey = o_custkey) AND (s_nationkey = n1.n_nationkey) AND (c_nationkey = n2.n_nationkey) AND (((n1.n_name = 'FRANCE') AND (n2.n_name = 'GERMANY')) OR ((n1.n_name = 'GERMANY') AND (n2.n_name = 'FRANCE'))) AND (l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)))) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"#,
    r#"SELECT o_year, (sum(CASE  WHEN ((nation = 'BRAZIL')) THEN (volume) ELSE 0 END) / sum(volume)) AS mkt_share FROM (SELECT date_part('YEAR', o_orderdate) AS o_year, (l_extendedprice * (1 - l_discount)) AS volume, n2.n_name AS nation FROM part , supplier , lineitem , orders , customer , nation AS n1 , nation AS n2 , region WHERE ((p_partkey = l_partkey) AND (s_suppkey = l_suppkey) AND (l_orderkey = o_orderkey) AND (o_custkey = c_custkey) AND (c_nationkey = n1.n_nationkey) AND (n1.n_regionkey = r_regionkey) AND (r_name = 'AMERICA') AND (s_nationkey = n2.n_nationkey) AND (o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)) AND (p_type = 'ECONOMY ANODIZED STEEL'))) AS all_nations GROUP BY o_year ORDER BY o_year"#,
    r#"SELECT nation, o_year, sum(amount) AS sum_profit FROM (SELECT n_name AS nation, date_part('YEAR', o_orderdate) AS o_year, ((l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity)) AS amount FROM part , supplier , lineitem , partsupp , orders , nation WHERE ((s_suppkey = l_suppkey) AND (ps_suppkey = l_suppkey) AND (ps_partkey = l_partkey) AND (p_partkey = l_partkey) AND (o_orderkey = l_orderkey) AND (s_nationkey = n_nationkey) AND (p_name ~~ '%green%'))) AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"#,
    r#"SELECT c_custkey, c_name, sum((l_extendedprice * (1 - l_discount))) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer , orders , lineitem , nation WHERE ((c_custkey = o_custkey) AND (l_orderkey = o_orderkey) AND (o_orderdate >= CAST('1993-10-01' AS DATE)) AND (o_orderdate < CAST('1994-01-01' AS DATE)) AND (l_returnflag = 'R') AND (c_nationkey = n_nationkey)) GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"#,
    r#"SELECT ps_partkey, sum((ps_supplycost * ps_availqty)) AS "value" FROM partsupp , supplier , nation WHERE ((ps_suppkey = s_suppkey) AND (s_nationkey = n_nationkey) AND (n_name = 'GERMANY')) GROUP BY ps_partkey HAVING (sum((ps_supplycost * ps_availqty)) > (SELECT (sum((ps_supplycost * ps_availqty)) * 0.0001000000) FROM partsupp , supplier , nation WHERE ((ps_suppkey = s_suppkey) AND (s_nationkey = n_nationkey) AND (n_name = 'GERMANY')))) ORDER BY "value" DESC"#,
    r#"SELECT l_shipmode, sum(CASE  WHEN (((o_orderpriority = '1-URGENT') OR (o_orderpriority = '2-HIGH'))) THEN (1) ELSE 0 END) AS high_line_count, sum(CASE  WHEN (((o_orderpriority != '1-URGENT') AND (o_orderpriority != '2-HIGH'))) THEN (1) ELSE 0 END) AS low_line_count FROM orders , lineitem WHERE ((o_orderkey = l_orderkey) AND (l_shipmode IN ('MAIL', 'SHIP')) AND (l_commitdate < l_receiptdate) AND (l_shipdate < l_commitdate) AND (l_receiptdate >= CAST('1994-01-01' AS DATE)) AND (l_receiptdate < CAST('1995-01-01' AS DATE))) GROUP BY l_shipmode ORDER BY l_shipmode"#,
    r#"SELECT c_count, count_star() AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM (customer LEFT JOIN orders ON (((c_custkey = o_custkey) AND (o_comment !~~ '%special%requests%')))) GROUP BY c_custkey) AS c_orders(c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC"#,
    r#"SELECT ((100.00 * sum(CASE  WHEN ((p_type ~~ 'PROMO%')) THEN ((l_extendedprice * (1 - l_discount))) ELSE 0 END)) / sum((l_extendedprice * (1 - l_discount)))) AS promo_revenue FROM lineitem , part WHERE ((l_partkey = p_partkey) AND (l_shipdate >= CAST('1995-09-01' AS DATE)) AND (l_shipdate < CAST('1995-10-01' AS DATE)))"#,
    r#"WITH revenue AS (SELECT l_suppkey AS supplier_no, sum((l_extendedprice * (1 - l_discount))) AS total_revenue FROM lineitem WHERE ((l_shipdate >= CAST('1996-01-01' AS DATE)) AND (l_shipdate < CAST('1996-04-01' AS DATE))) GROUP BY supplier_no) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier , revenue WHERE ((s_suppkey = supplier_no) AND (total_revenue = (SELECT max(total_revenue) FROM revenue))) ORDER BY s_suppkey"#,
    r#"SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp , part WHERE ((p_partkey = ps_partkey) AND (p_brand != 'Brand#45') AND (p_type !~~ 'MEDIUM POLISHED%') AND (p_size IN (49, 14, 23, 45, 19, 3, 36, 9)) AND (NOT (ps_suppkey = ANY(SELECT s_suppkey FROM supplier WHERE (s_comment ~~ '%Customer%Complaints%'))))) GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"#,
    r#"SELECT (sum(l_extendedprice) / 7.0) AS avg_yearly FROM lineitem , part WHERE ((p_partkey = l_partkey) AND (p_brand = 'Brand#23') AND (p_container = 'MED BOX') AND (l_quantity < (SELECT (0.2 * avg(l_quantity)) FROM lineitem WHERE (l_partkey = p_partkey))))"#,
    r#"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) FROM customer , orders , lineitem WHERE ((o_orderkey = ANY(SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING (sum(l_quantity) > 300))) AND (c_custkey = o_custkey) AND (o_orderkey = l_orderkey)) GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"#,
    r#"SELECT sum((l_extendedprice * (1 - l_discount))) AS revenue FROM lineitem , part WHERE (((p_partkey = l_partkey) AND (p_brand = 'Brand#12') AND (p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')) AND (l_quantity >= 1) AND (l_quantity <= 11) AND (p_size BETWEEN 1 AND 5) AND (l_shipmode IN ('AIR', 'AIR REG')) AND (l_shipinstruct = 'DELIVER IN PERSON')) OR ((p_partkey = l_partkey) AND (p_brand = 'Brand#23') AND (p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')) AND (l_quantity >= 10) AND (l_quantity <= 20) AND (p_size BETWEEN 1 AND 10) AND (l_shipmode IN ('AIR', 'AIR REG')) AND (l_shipinstruct = 'DELIVER IN PERSON')) OR ((p_partkey = l_partkey) AND (p_brand = 'Brand#34') AND (p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')) AND (l_quantity >= 20) AND (l_quantity <= 30) AND (p_size BETWEEN 1 AND 15) AND (l_shipmode IN ('AIR', 'AIR REG')) AND (l_shipinstruct = 'DELIVER IN PERSON')))"#,
    r#"SELECT s_name, s_address FROM supplier , nation WHERE ((s_suppkey = ANY(SELECT ps_suppkey FROM partsupp WHERE ((ps_partkey = ANY(SELECT p_partkey FROM part WHERE (p_name ~~ 'forest%'))) AND (ps_availqty > (SELECT (0.5 * sum(l_quantity)) FROM lineitem WHERE ((l_partkey = ps_partkey) AND (l_suppkey = ps_suppkey) AND (l_shipdate >= CAST('1994-01-01' AS DATE)) AND (l_shipdate < CAST('1995-01-01' AS DATE)))))))) AND (s_nationkey = n_nationkey) AND (n_name = 'CANADA')) ORDER BY s_name"#,
    r#"SELECT s_name, count_star() AS numwait FROM supplier , lineitem AS l1 , orders , nation WHERE ((s_suppkey = l1.l_suppkey) AND (o_orderkey = l1.l_orderkey) AND (o_orderstatus = 'F') AND (l1.l_receiptdate > l1.l_commitdate) AND EXISTS(SELECT * FROM lineitem AS l2 WHERE ((l2.l_orderkey = l1.l_orderkey) AND (l2.l_suppkey != l1.l_suppkey))) AND (NOT EXISTS(SELECT * FROM lineitem AS l3 WHERE ((l3.l_orderkey = l1.l_orderkey) AND (l3.l_suppkey != l1.l_suppkey) AND (l3.l_receiptdate > l3.l_commitdate)))) AND (s_nationkey = n_nationkey) AND (n_name = 'SAUDI ARABIA')) GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"#,
    r#"SELECT cntrycode, count_star() AS numcust, sum(c_acctbal) AS totacctbal FROM (SELECT "substring"(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer WHERE (("substring"(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND (c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE ((c_acctbal > 0.00) AND ("substring"(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17'))))) AND (NOT EXISTS(SELECT * FROM orders WHERE (o_custkey = c_custkey))))) AS custsale GROUP BY cntrycode ORDER BY cntrycode"#,
    r#"SELECT * FROM type_matrix ORDER BY id"#,
    r#"SELECT id, c_dec, c_date, c_ts FROM type_matrix WHERE (c_i32 > 0) ORDER BY id"#,
    r#"SELECT count_star(), sum(c_i64), min(c_f64), max(c_varchar), avg(c_dec) FROM type_matrix"#,
    r#"SELECT c_bool, count_star() FROM type_matrix GROUP BY c_bool ORDER BY c_bool"#,
    r#"SELECT id, c_list, c_struct FROM type_matrix WHERE (c_list IS NOT NULL) ORDER BY id"#,
    r#"SELECT id FROM type_matrix WHERE (c_varchar ~~ 'b%') ORDER BY id"#,
    r#"SELECT id, (c_date + "system".main."add"("system".main."add"("system".main.to_months(0), "system".main.to_days(1)), "system".main.to_microseconds(CAST(0 AS BIGINT)))) AS next_day FROM type_matrix ORDER BY id"#,
    r#"SELECT id, CAST(c_ts AS DATE) AS d FROM type_matrix ORDER BY id"#,
    r#"SELECT id, length(c_varchar), upper(c_varchar) FROM type_matrix ORDER BY id"#,
    r#"SELECT (max(c_ts) - min(c_ts)) AS span FROM type_matrix"#,
];

/// What the Quack table provider sent in seeded mode.
const PROVIDER_QUERIES: &[&str] = &[
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower('main') AND lower(table_name) = lower('quack_ext_a') ORDER BY ordinal_position"#,
    r#"SELECT "id" FROM "main"."quack_ext_a" LIMIT 0"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower('main') AND lower(table_name) = lower('quack_ext_b') ORDER BY ordinal_position"#,
    r#"SELECT "id", "name" FROM "main"."quack_ext_b" LIMIT 0"#,
    r#"SELECT "quack_ext_b"."name" FROM "main"."quack_ext_a" INNER JOIN "main"."quack_ext_b" ON ("quack_ext_a"."id" = "quack_ext_b"."id") WHERE ("quack_ext_a"."id" > 0) ORDER BY "quack_ext_b"."name" ASC NULLS LAST"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('no_such_table') ORDER BY ordinal_position"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_exhaust') ORDER BY ordinal_position"#,
    r#"SELECT "k" FROM "quack_exhaust" LIMIT 0"#,
    r#"SELECT "quack_exhaust"."k" FROM "quack_exhaust""#,
    r#"SELECT "quack_exhaust"."k" FROM "quack_exhaust" WHERE ("quack_exhaust"."k" < 10)"#,
    r#"SELECT 1 FROM "quack_exhaust""#,
    r#"CREATE TABLE overflow_329331777 (v BIGINT)"#,
    r#"INSERT INTO overflow_329331777 VALUES (9223372036854775807), (9223372036854775807)"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('overflow_329331777') ORDER BY ordinal_position"#,
    r#"SELECT "v" FROM "overflow_329331777" LIMIT 0"#,
    r#"SELECT sum("overflow_329331777"."v") AS "total" FROM "overflow_329331777""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_orders') ORDER BY ordinal_position"#,
    r#"SELECT "id", "customer_id", "amount" FROM "quack_orders" LIMIT 0"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_customers') ORDER BY ordinal_position"#,
    r#"SELECT "id", "name" FROM "quack_customers" LIMIT 0"#,
    r#"SELECT "quack_customers"."name", sum("quack_orders"."amount") AS "total" FROM "quack_orders" INNER JOIN "quack_customers" ON ("quack_orders"."customer_id" = "quack_customers"."id") GROUP BY "quack_customers"."name" ORDER BY "quack_customers"."name" ASC NULLS LAST"#,
    r#"SELECT "quack_orders"."id", "quack_orders"."customer_id", "quack_orders"."amount" FROM "quack_orders""#,
    r#"SELECT "quack_customers"."id", "quack_customers"."name" FROM "quack_customers""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_runtime_filters') ORDER BY ordinal_position"#,
    r#"SELECT "k" FROM "quack_runtime_filters" LIMIT 0"#,
    r#"SELECT "quack_runtime_filters"."k" FROM "quack_runtime_filters""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_pushdown') ORDER BY ordinal_position"#,
    r#"SELECT "id", "i", "u", "dec", "d", "ts", "ts_s", "ts_ns", "tstz", "b", "s", "f" FROM "quack_pushdown" LIMIT 0"#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."i", "quack_pushdown"."u", "quack_pushdown"."dec", "quack_pushdown"."d", "quack_pushdown"."ts", "quack_pushdown"."ts_s", "quack_pushdown"."ts_ns", "quack_pushdown"."tstz", "quack_pushdown"."b", "quack_pushdown"."s", "quack_pushdown"."f" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."i" = 3) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."i" <> 3) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE (("quack_pushdown"."i" < 3) AND "quack_pushdown"."i" IS NOT NULL) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE (("quack_pushdown"."i" = 1) OR ("quack_pushdown"."i" = 5)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE (("quack_pushdown"."i" <> 1) AND ("quack_pushdown"."i" <> 5)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."i" <= 2) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE (("quack_pushdown"."i" > 1) OR "quack_pushdown"."b") ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."u" >= 5) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."u" = 18446744073709551615) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."dec" >= 1.50) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."dec" < 0.00) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."d" = CAST('2024-06-30' AS DATE)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."d" > CAST('2024-01-01' AS DATE)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."ts" > CAST('2024-01-01 00:00:00' AS TIMESTAMP)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."ts" <= CAST('2024-06-30 12:00:00.500' AS TIMESTAMP)) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE NOT "quack_pushdown"."b" ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE "quack_pushdown"."s" IS NULL ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE "quack_pushdown"."f" IS NOT NULL ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."s" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."f" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."ts_ns" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."tstz" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."i" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."i" > 2) ORDER BY "id" ASC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."ts" FROM "quack_pushdown""#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" LIMIT 3"#,
    r#"SELECT "quack_pushdown"."id" FROM "quack_pushdown" WHERE ("quack_pushdown"."i" > 1) LIMIT 2"#,
    r#"SELECT 1 FROM "quack_pushdown""#,
    r#"SELECT 1 FROM "quack_pushdown" WHERE ("quack_pushdown"."i" >= 2)"#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."i" FROM "quack_pushdown" ORDER BY "i" DESC NULLS LAST"#,
    r#"SELECT "quack_pushdown"."id", "quack_pushdown"."i" FROM "quack_pushdown" ORDER BY "i" ASC NULLS LAST, "id" ASC NULLS LAST"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_nullability') ORDER BY ordinal_position"#,
    r#"SELECT "id", "name", "note", "amount" FROM "quack_nullability" LIMIT 0"#,
    r#"SELECT "quack_nullability"."id", "quack_nullability"."name", "quack_nullability"."note", "quack_nullability"."amount" FROM "quack_nullability""#,
    r#"SELECT "quack_nullability"."id", "quack_nullability"."note", "quack_nullability"."amount" FROM "quack_nullability" WHERE ("quack_nullability"."id" > 0)"#,
    r#"SELECT 1 FROM "quack_nullability""#,
    r#"SELECT "quack_nullability"."id", "quack_nullability"."amount" FROM "quack_nullability""#,
    r#"SELECT "quack_nullability"."amount", "quack_nullability"."note", "quack_nullability"."id" FROM "quack_nullability" WHERE ("quack_nullability"."id" > 0)"#,
    r#"SELECT count(1) AS "count(*)" FROM "quack_nullability""#,
    r#"SELECT "quack_nullability"."amount", "quack_nullability"."id" FROM "quack_nullability""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_no_runtime') ORDER BY ordinal_position"#,
    r#"SELECT "answer" FROM "quack_no_runtime" LIMIT 0"#,
    r#"SELECT "quack_no_runtime"."answer" FROM "quack_no_runtime""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_types') ORDER BY ordinal_position"#,
    r#"SELECT "c_bool", "c_i8", "c_i16", "c_i32", "c_i64", "c_u8", "c_u16", "c_u32", "c_u64", "c_f32", "c_f64", "c_dec4", "c_dec18", "c_dec38", "c_varchar", "c_blob", "c_date", "c_time", "c_time_ns", "c_ts", "c_ts_s", "c_ts_ms", "c_ts_ns", "c_tstz", "c_interval", "c_list", "c_list_str", "c_struct", "c_map", "c_array" FROM "quack_types" LIMIT 0"#,
    r#"SELECT "quack_types"."c_bool", "quack_types"."c_i8", "quack_types"."c_i16", "quack_types"."c_i32", "quack_types"."c_i64", "quack_types"."c_u8", "quack_types"."c_u16", "quack_types"."c_u32", "quack_types"."c_u64", "quack_types"."c_f32", "quack_types"."c_f64", "quack_types"."c_dec4", "quack_types"."c_dec18", "quack_types"."c_dec38", "quack_types"."c_varchar", "quack_types"."c_blob", "quack_types"."c_date", "quack_types"."c_time", "quack_types"."c_time_ns", "quack_types"."c_ts", "quack_types"."c_ts_s", "quack_types"."c_ts_ms", "quack_types"."c_ts_ns", "quack_types"."c_tstz", "quack_types"."c_interval", "quack_types"."c_list", "quack_types"."c_list_str", "quack_types"."c_struct", "quack_types"."c_map", "quack_types"."c_array" FROM "quack_types" ORDER BY "c_bool" ASC NULLS LAST"#,
    r#"SELECT "quack_types"."c_bool", "quack_types"."c_i8", "quack_types"."c_i16", "quack_types"."c_i32", "quack_types"."c_i64", "quack_types"."c_u8", "quack_types"."c_u16", "quack_types"."c_u32", "quack_types"."c_u64", "quack_types"."c_f32", "quack_types"."c_f64", "quack_types"."c_dec4", "quack_types"."c_dec18", "quack_types"."c_dec38", "quack_types"."c_varchar", "quack_types"."c_blob", "quack_types"."c_date", "quack_types"."c_time", "quack_types"."c_time_ns", "quack_types"."c_ts", "quack_types"."c_ts_s", "quack_types"."c_ts_ms", "quack_types"."c_ts_ns", "quack_types"."c_tstz", "quack_types"."c_interval", "quack_types"."c_list", "quack_types"."c_list_str", "quack_types"."c_struct", "quack_types"."c_map", "quack_types"."c_array" FROM "quack_types" ORDER BY "quack_types"."c_bool" ASC NULLS LAST"#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('MixedCase_seed') ORDER BY ordinal_position"#,
    r#"SELECT "id" FROM "MixedCase_seed" LIMIT 0"#,
    r#"SELECT "MixedCase_seed"."id" FROM "MixedCase_seed""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('mixedcase_seed') ORDER BY ordinal_position"#,
    r#"SELECT "id" FROM "mixedcase_seed" LIMIT 0"#,
    r#"SELECT "mixedcase_seed"."id" FROM "mixedcase_seed""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower('main') AND lower(table_name) = lower('MixedCase_seed') ORDER BY ordinal_position"#,
    r#"SELECT "id" FROM "main"."MixedCase_seed" LIMIT 0"#,
    r#"SELECT "MixedCase_seed"."id" FROM "main"."MixedCase_seed""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('quack_view') ORDER BY ordinal_position"#,
    r#"SELECT "id" FROM "quack_view" LIMIT 0"#,
    r#"SELECT "quack_view"."id" FROM "quack_view""#,
    r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE lower(table_catalog) = lower(current_database()) AND lower(table_schema) = lower(current_schema()) AND lower(table_name) = lower('missing_699824163') ORDER BY ordinal_position"#,
];

/// What the `quack_protocol` client's read-only live tests sent.
const CLIENT_QUERIES: &[&str] = &[
    r#"SELECT 1::INTEGER AS id, 'x'::VARCHAR AS label WHERE 1 = 0"#,
    r#"SELECT i FROM range(5000) t(i) ORDER BY i"#,
    r#"SELECT 8::INTEGER"#,
    r#"
            SELECT *
            FROM (
              VALUES
                (1::INTEGER, 'one'::VARCHAR),
                (2::INTEGER, 'two'::VARCHAR)
            ) AS t(id, label)
            ORDER BY id
            "#,
    r#"SELECT 1::INTEGER AS id, 'x'::VARCHAR AS label WHERE FALSE"#,
    r#"
            SELECT
              [1, NULL, 3]::INTEGER[] AS ints,
              [[1, 2], [3, 4]]::INTEGER[][] AS nested_ints,
              {'x': 1::INTEGER, 'y': 'one'::VARCHAR} AS point,
              {'label': 'bag'::VARCHAR, 'items': [10, 20]::INTEGER[]} AS nested_struct,
              map(['a', 'b'], [1, 2]) AS map_v,
              array_value(7, 8, 9)::INTEGER[3] AS fixed_v
            "#,
    r#"SELECT 7::INTEGER AS id, 'seven'::VARCHAR AS label, [1, 2, 3]::INTEGER[] AS values"#,
    r#"SELECT 8::INTEGER AS id, 'eight'::VARCHAR AS label"#,
];

const TPCH_SCHEMA: &str = "
CREATE TABLE customer(c_custkey BIGINT NOT NULL, c_name VARCHAR NOT NULL, c_address VARCHAR NOT NULL, c_nationkey INTEGER NOT NULL, c_phone VARCHAR NOT NULL, c_acctbal DECIMAL(15,2) NOT NULL, c_mktsegment VARCHAR NOT NULL, c_comment VARCHAR NOT NULL);
CREATE TABLE lineitem(l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);
CREATE TABLE nation(n_nationkey INTEGER NOT NULL, n_name VARCHAR NOT NULL, n_regionkey INTEGER NOT NULL, n_comment VARCHAR NOT NULL);
CREATE TABLE orders(o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderstatus VARCHAR NOT NULL, o_totalprice DECIMAL(15,2) NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority VARCHAR NOT NULL, o_clerk VARCHAR NOT NULL, o_shippriority INTEGER NOT NULL, o_comment VARCHAR NOT NULL);
CREATE TABLE part(p_partkey BIGINT NOT NULL, p_name VARCHAR NOT NULL, p_mfgr VARCHAR NOT NULL, p_brand VARCHAR NOT NULL, p_type VARCHAR NOT NULL, p_size INTEGER NOT NULL, p_container VARCHAR NOT NULL, p_retailprice DECIMAL(15,2) NOT NULL, p_comment VARCHAR NOT NULL);
CREATE TABLE partsupp(ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT NOT NULL, ps_supplycost DECIMAL(15,2) NOT NULL, ps_comment VARCHAR NOT NULL);
CREATE TABLE region(r_regionkey INTEGER NOT NULL, r_name VARCHAR NOT NULL, r_comment VARCHAR NOT NULL);
CREATE TABLE supplier(s_suppkey BIGINT NOT NULL, s_name VARCHAR NOT NULL, s_address VARCHAR NOT NULL, s_nationkey INTEGER NOT NULL, s_phone VARCHAR NOT NULL, s_acctbal DECIMAL(15,2) NOT NULL, s_comment VARCHAR NOT NULL);
";

/// The type matrix of `differential.py`, as DataFusion reads its Parquet file.
fn type_matrix() -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("c_bool", DataType::Boolean, true),
        Field::new("c_i8", DataType::Int8, true),
        Field::new("c_i16", DataType::Int16, true),
        Field::new("c_i32", DataType::Int32, true),
        Field::new("c_i64", DataType::Int64, true),
        Field::new("c_u8", DataType::UInt8, true),
        Field::new("c_u32", DataType::UInt32, true),
        Field::new("c_f32", DataType::Float32, true),
        Field::new("c_f64", DataType::Float64, true),
        Field::new("c_dec", DataType::Decimal128(12, 2), true),
        Field::new("c_dec38", DataType::Decimal128(38, 3), true),
        Field::new("c_varchar", DataType::Utf8View, true),
        Field::new("c_blob", DataType::BinaryView, true),
        Field::new("c_date", DataType::Date32, true),
        Field::new(
            "c_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("c_time", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new(
            "c_list",
            DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "c_struct",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8View, true),
            ])),
            true,
        ),
    ]));
    MemTable::try_new(
        Arc::clone(&schema),
        vec![vec![RecordBatch::new_empty(schema)]],
    )
    .unwrap()
}

async fn server() -> (String, tokio::task::JoinHandle<()>) {
    let config = SessionConfig::new()
        .with_default_catalog_and_schema("memory", "main")
        .with_create_default_catalog_and_schema(true);
    let ctx = SessionContext::new_with_config(config);
    load(&ctx, Seed::ProviderFixtures).await.unwrap();
    for statement in TPCH_SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        ctx.sql(statement).await.unwrap().collect().await.unwrap();
    }
    ctx.register_table(TableReference::bare("type_matrix"), Arc::new(type_matrix()))
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("quack:{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        QuackServer::new(Arc::new(ctx))
            .with_options(ServerOptions::new().with_token("replay-token"))
            .serve_with_listener(listener)
            .await
            .unwrap();
    });
    (uri, task)
}

/// Replays `queries` in one session; every one must plan and execute.
async fn replay(queries: &[&str], duckdb_client: bool) {
    let (uri, server) = server().await;
    let client = QuackClient::connect(
        &uri,
        QuackClientOptions {
            auth_token: Some("replay-token".into()),
            // DuckDB reports its version, and gets DuckDB's result types
            client_duckdb_version: duckdb_client.then(|| "v2.0.0".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut failures = Vec::new();
    for sql in queries {
        let result = match client.query(sql, None).await {
            Ok(stream) => stream
                .into_chunks()
                .1
                .try_collect::<Vec<_>>()
                .await
                .map(drop),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            failures.push(format!("{sql}\n  -> {error}"));
        }
    }
    server.abort();
    assert!(
        failures.is_empty(),
        "{} failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn duckdb_attach_queries_replay() {
    replay(DUCKDB_ATTACH_QUERIES, true).await;
}

#[tokio::test]
async fn provider_queries_replay() {
    replay(PROVIDER_QUERIES, false).await;
}

#[tokio::test]
async fn client_queries_replay() {
    replay(CLIENT_QUERIES, false).await;
}
