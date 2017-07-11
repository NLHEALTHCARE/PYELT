DROP INDEX sor_performance_tests.ix_random_data_temp_hash_key;
DROP INDEX sor_performance_tests.ix_random_data_temp_hash_hash;
DROP INDEX sor_performance_tests.ix_random_data_hstage_key;
DROP INDEX sor_performance_tests.ix_random_data_hstage_hash;
DROP INDEX sor_performance_tests.ix_random_data_hstage_runid;

CREATE INDEX ix_random_data_temp_hash_key
   ON sor_performance_tests.random_data_temp_hash USING btree (id ASC NULLS LAST);
CREATE INDEX ix_random_data_temp_hash_hash
   ON sor_performance_tests.random_data_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_random_data_hstage_key
   ON sor_performance_tests.random_data_hstage USING btree (id ASC NULLS LAST);
CREATE INDEX ix_random_data_hstage_hash
   ON sor_performance_tests.random_data_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_random_data_hstage_runid
   ON sor_performance_tests.random_data_hstage USING btree (_runid ASC NULLS LAST);