CREATE INDEX ix_afdeling_temp_hash_key
   ON sor_timeff.afdeling_temp_hash USING btree (pefinrint ASC NULLS LAST);
CREATE INDEX ix_afdeling_temp_hash_hash
   ON sor_timeff.afdeling_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_afdeling_hstage_key
   ON sor_timeff.afdeling_hstage USING btree (pefinrint ASC NULLS LAST);
CREATE INDEX ix_afdeling_hstage_hash
   ON sor_timeff.afdeling_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_afdeling_hstage_runid
   ON sor_timeff.afdeling_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_afspraak_deelnemer_temp_hash_key
   ON sor_timeff.afspraak_deelnemer_temp_hash USING btree (ifam_mdw_id, ifev_id, ifam_hoedanigheid, ifam_eigenaar_type);
CREATE INDEX ix_afspraak_deelnemer_temp_hash_hash
   ON sor_timeff.afspraak_deelnemer_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_afspraak_deelnemer_hstage_key
   ON sor_timeff.afspraak_deelnemer_hstage USING btree (ifam_mdw_id, ifev_id, ifam_hoedanigheid, ifam_eigenaar_type);
CREATE INDEX ix_afspraak_deelnemer_hstage_hash
   ON sor_timeff.afspraak_deelnemer_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_afspraak_deelnemer_hstage_runid
   ON sor_timeff.afspraak_deelnemer_hstage USING btree (_runid ASC NULLS LAST);
-------------------------
CREATE INDEX ix_contactgegevens_temp_hash_key
   ON sor_timeff.contactgegevens_temp_hash USING btree (rebenrint ASC NULLS LAST);
CREATE INDEX ix_contactgegevens_temp_hash_hash
   ON sor_timeff.contactgegevens_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_contactgegevens_hstage_key
   ON sor_timeff.contactgegevens_hstage USING btree (rebenrint ASC NULLS LAST);
CREATE INDEX ix_contactgegevens_hstage_hash
   ON sor_timeff.contactgegevens_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_contactgegevens_hstage_runid
   ON sor_timeff.contactgegevens_hstage USING btree (_runid ASC NULLS LAST);
-------------------------
CREATE INDEX ix_factuurregel_temp_hash_key
   ON sor_timeff.factuurregel_temp_hash USING btree (ivrf_vf_id, ivrf_pf_id);
CREATE INDEX ix_factuurregel_temp_hash_hash
   ON sor_timeff.factuurregel_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_factuurregel_hstage_key
   ON sor_timeff.factuurregel_hstage USING btree (ivrf_vf_id, ivrf_pf_id);
CREATE INDEX ix_factuurregel_hstage_hash
   ON sor_timeff.factuurregel_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_factuurregel_hstage_runid
   ON sor_timeff.factuurregel_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_medewerker_temp_hash_key
   ON sor_timeff.medewerker_temp_hash USING btree (ifmw_id ASC NULLS LAST);
CREATE INDEX ix_medewerker_temp_hash_hash
   ON sor_timeff.medewerker_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_medewerker_hstage_key
   ON sor_timeff.medewerker_hstage USING btree (ifmw_id ASC NULLS LAST);
CREATE INDEX ix_medewerker_hstage_hash
   ON sor_timeff.medewerker_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_medewerker_hstage_runid
   ON sor_timeff.medewerker_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_persoon_temp_hash_key
   ON sor_timeff.persoon_temp_hash USING btree (ifct_id ASC NULLS LAST);
CREATE INDEX ix_persoon_temp_hash_hash
   ON sor_timeff.persoon_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_persoon_hstage_key
   ON sor_timeff.persoon_hstage USING btree (ifct_id ASC NULLS LAST);
CREATE INDEX ix_persoon_hstage_hash
   ON sor_timeff.persoon_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_persoon_hstage_runid
   ON sor_timeff.persoon_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_subtraject_temp_hash_key
   ON sor_timeff.subtraject_temp_hash USING btree (ifst_id ASC NULLS LAST);
CREATE INDEX ix_subtraject_temp_hash_hash
   ON sor_timeff.subtraject_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_subtraject_hstage_key
   ON sor_timeff.subtraject_hstage USING btree (ifst_id ASC NULLS LAST);
CREATE INDEX ix_subtraject_hstage_hash
   ON sor_timeff.subtraject_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_subtraject_hstage_runid
   ON sor_timeff.subtraject_hstage USING btree (_runid ASC NULLS LAST);
-------------------------
CREATE INDEX ix_tarieven_temp_hash_key
   ON sor_timeff.tarieven_temp_hash USING btree (ifck_ct_code, ifck_info_value9, ifck_info_value10, ifck_startdatum,ifck_einddatum);
CREATE INDEX ix_tarieven_temp_hash_hash
   ON sor_timeff.tarieven_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_tarieven_hstage_key
   ON sor_timeff.tarieven_hstage USING btree (ifck_ct_code, ifck_info_value9, ifck_info_value10, ifck_startdatum,ifck_einddatum);
CREATE INDEX ix_tarieven_hstage_hash
   ON sor_timeff.tarieven_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_tarieven_hstage_runid
   ON sor_timeff.tarieven_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_vestiging_temp_hash_key
   ON sor_timeff.vestiging_temp_hash USING btree (pefinrint ASC NULLS LAST);
CREATE INDEX ix_vestiging_temp_hash_hash
   ON sor_timeff.vestiging_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_vestiging_hstage_key
   ON sor_timeff.vestiging_hstage USING btree (pefinrint ASC NULLS LAST);
CREATE INDEX ix_vestiging_hstage_hash
   ON sor_timeff.vestiging_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_vestiging_hstage_runid
   ON sor_timeff.vestiging_hstage USING btree (_runid ASC NULLS LAST);
----------------------
CREATE INDEX ix_zorgactiviteit_temp_hash_key
   ON sor_timeff.zorgactiviteit_temp_hash USING btree (ifza_iddot ASC NULLS LAST);
CREATE INDEX ix_zorgactiviteit_temp_hash_hash
   ON sor_timeff.zorgactiviteit_temp_hash USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_zorgactiviteit_hstage_key
   ON sor_timeff.zorgactiviteit_hstage USING btree (ifza_iddot ASC NULLS LAST);
CREATE INDEX ix_zorgactiviteit_hstage_hash
   ON sor_timeff.zorgactiviteit_hstage USING btree (_hash ASC NULLS LAST);
CREATE INDEX ix_zorgactiviteit_hstage_runid
   ON sor_timeff.zorgactiviteit_hstage USING btree (_runid ASC NULLS LAST);
----------------------
