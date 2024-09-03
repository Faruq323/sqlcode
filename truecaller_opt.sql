INSERT INTO `${truecall_tgt_project_id}.${truecall_tgt_dataset_name}.${truecall_target_tblname}`
    (
    environment,
    end_location_lat,
    end_location_lon,
    model,
    make,
    service_type,
    start_m_tmsi,
    start_time,
    duration,
    end_time,
    start_type,
    end_type,
    final_disposition,
    start_cell,
    start_enb,
    end_cell,
    end_enb,
    s1_release_cause,
    rsrp_dbm,
    rsrq_db,
    end_timing_advance_meters,
    intra_frequency_intra_enb_ho_attempts,
 intra_frequency_intra_enb_ho_failures,
    inter_frequency_intra_enb_ho_attempts,
    inter_frequency_intra_enb_ho_failures,
    intra_frequency_x2_ho_prep_attempts,
    intra_frequency_x2_ho_prep_failures,
    inter_frequency_x2_ho_exec_failures,
    inter_frequency_x2_ho_prep_attempts,
    intra_frequency_x2_ho_exec_failures,
    inter_frequency_s1_ho_prep_attempts,
    inter_frequency_s1_ho_prep_failures,
    intra_frequency_s1_ho_exec_failures,
    intra_frequency_s1_ho_prep_attempts,
    intra_frequency_s1_ho_prep_failures,
    inter_frequency_s1_ho_exec_failures,
    irat_ho_attempts,
    last_volte_erab_duration,
    last_volte_erab_end_time,
    last_volte_erab_release_cause,
    last_volte_erab_start_time,
    volte_erab_with_emergency_arp,
    initial_nas_request,
    initial_nas_response,
    oem_rrc_setup_cause,
    oem_s1_setup_cause,
    oem_s1_release_cause,
 oem_internal_release_cause,
    release_with_data_lost,
    data_lost,
    mean_cqi,
    mac_volume_dl_bytes,
    mac_volume_ul_bytes,
    pdcp_volume_dl_bytes,
    pdcp_volume_ul_bytes,
    mean_pdcp_drb_throughput_dl_kbps,
    mean_pdcp_drb_throughput_ul_kbps,
    enb_cfcq,
    dl_ttis_ue_scheduled,
    end_earfcn_dl,
    qci_list,
    rrc_re_establishment_failures,
    rrc_re_establishment_successes,
    num_of_neighbors,
    n1_rsrp_dbm,
    n2_rsrp_dbm,
    n3_rsrp_dbm,
    pusch_sinr_db,
    ue_power_headroom_db,
    ta_distance_meters,
    intersite_distance_meters,
    imei,
    imsi,
    volte_erab_attempts,
    volte_erab_drops,
    volte_erab_duration,
    volte_erab_failed_attempts,
 volte_erab_release_attempts,
    rlc_dl_throughput_kbps,
    rlc_pdu_dl_retransmit_rate_percentage,
    rlc_pdu_dl_retransmitted_volume_bytes,
    rlc_pdu_dl_volume_bytes,
    rlc_pdu_ul_volume_bytes,
    rlc_sdu_ul_volume_kbytes,
    rlc_ul_throughput_kbps,
    mobile_number,
    mean_ul_sinr_db,
    vendor,
    confidence,
    nr_serving_cell,
    nr_serving_pci,
    nr_serving_arfcn,
    en_dc_duration,
    en_dc_duration_rate_percentage,
    maximum_number_of_carrier_components,
    average_number_of_carrier_components,
    en_dc_sgnb_addition_attempts,
    en_dc_sgnb_addition_failures,
    en_dc_sgnb_drops,
    last_5g_en_dc_release_cause,
    en_dc_downlink_volume_bytes,
    en_dc_uplink_volume_bytes,
    time_of_last_rrc_measurement_reports_with_5g_nr_cell,
    nr_last_measurement_cell,
    nr_last_measurement_pci,
    nr_last_measurement_arfcn,
    nr_rsrp_dbm,
 nr_rsrq_db,
    nr_dl_sinr_db,
    en_dc_sgnb_change_attempts,
    en_dc_sgnb_change_failures,
    en_dc_sgnb_modification_attempts,
    en_dc_sgnb_modification_failures,
    radio_type,
    call_release_cause,
    s1_u_ul_data_volume_bytes,
    s1_u_dl_data_volume_bytes,
    mean_uplink_ip_throughput_kbps,
    mean_downlink_ipthroughput_kbps,
    call_type,
    last_rrc_reestablishment_case,
    last_cqi,
    csl_event,
    call_segment_duration,
    last_ue_tx_power,
    csfb,
    csfb_result,
    csfb_type,
    ue_5g_capable,
    nr_final_disposition,
    lte_carrier_aggregation_service_time,
    nr_carrier_aggregation_service_time,
    intra_frequency_x2_ho_exec_attempts,
    inter_frequency_x2_ho_exec_attempts,
    rrc_re_establishment_attempts,
    nr_scg_failure_type,
    mean_mac_throughput_ul_kbps,
 mean_mac_throughput_dl_kbps,
    composite_call,
    fragment_id,
    global_id,
    erab_drops,
    nr_n1_pci,
    nr_n1_cell,
    nr_n1_rsrp_dbm,
    nr_n1_dl_sinr_db,
    nr_n2_pci,
    nr_n2_cell,
    nr_n2_rsrp_dbm,
    nr_n2_dl_sinr_db,
    average_number_of_nr_carrier_components,
    nr_scell_list,
    nr_scell_arfcn_list,
    lte_scell_list,
    lte_scell_arfcn_list,
    lte_carrier_aggregation_service_rate_percentage,
    erab_attempts,
    erab_failed_attempts,
    erab_normal_releases,
    erab_release_attempts,
    erab_successes,
    lte_mkt,
    lte_base_mkt,
    submkt,
    duration_seconds,
    call_segment_duration_seconds,
    en_dc_duration_seconds,
    volte_erab_duration_seconds,
    last_volte_erab_duration_seconds,
    h3_06,
    h3_07,
    h3_08,
    h3_09,
    mgrs_1m,
    conn_tech_type,
    conn_tech_type_desc,
    enb_gnb_id,
    start_cell_sector,
    channel,
    end_cell_sector,
    new_carr,
    trans_dt,
    hr,
    submkt_fname,
    process_dt
    )
    
WITH data1 as(
SELECT
    environment,
    end_location_lat,
    end_location_lon,
    model,
    make,
    service_type,
    start_m_tmsi,
    safe_cast(start_time as DATETIME),
    duration ,
    safe_cast(end_time as DATETIME),
    start_type,
    end_type,
    final_disposition,
    safe_cast(start_cell AS STRING) start_cell,
    start_enb,
    safe_cast(end_cell AS STRING),
    end_enb,
    s1_release_cause,
    rsrp_dbm,
    rsrq_db,
    end_timing_advance_meters,
    SAFE_CAST(intra_frequency_intra_enb_ho_attempts  as INT64),
    SAFE_CAST(intra_frequency_intra_enb_ho_failures  as INT64),
    SAFE_CAST(inter_frequency_intra_enb_ho_attempts  as INT64),
    SAFE_CAST(inter_frequency_intra_enb_ho_failures  as INT64),
    SAFE_CAST(intra_frequency_x2_ho_prep_attempts  as INT64),
    SAFE_CAST(intra_frequency_x2_ho_prep_failures  as INT64),
    SAFE_CAST(inter_frequency_x2_ho_exec_failures  as INT64),
    SAFE_CAST(inter_frequency_x2_ho_prep_attempts  as INT64),
    SAFE_CAST(intra_frequency_x2_ho_exec_failures  as INT64),
    SAFE_CAST(inter_frequency_s1_ho_prep_attempts  as INT64),
    SAFE_CAST(inter_frequency_s1_ho_prep_failures  as INT64),
    SAFE_CAST(intra_frequency_s1_ho_exec_failures  as INT64),
    SAFE_CAST(intra_frequency_s1_ho_prep_attempts  as INT64),
    SAFE_CAST(intra_frequency_s1_ho_prep_failures  as INT64),
    SAFE_CAST(inter_frequency_s1_ho_exec_failures  as INT64),
    SAFE_CAST(irat_ho_attempts as INT64),
    last_volte_erab_duration ,
    CAST(last_volte_erab_end_time as DATETIME),
    last_volte_erab_release_cause,
    last_volte_erab_start_time,
    SAFE_CAST(volte_erab_with_emergency_arp as FLOAT64),
    initial_nas_request,
    initial_nas_response,
    oem_rrc_setup_cause,
    oem_s1_setup_cause,
    oem_s1_release_cause,
    oem_internal_release_cause,
    release_with_data_lost,
    data_lost,
    SAFE_CAST(mean_cqi as FLOAT64),
    SAFE_CAST(mac_volume_dl_bytes AS INT64),
    SAFE_CAST(mac_volume_ul_bytes AS INT64),
    SAFE_CAST(pdcp_volume_dl_bytes AS INT64),
    SAFE_CAST(pdcp_volume_ul_bytes AS INT64),
    SAFE_CAST(mean_pdcp_drb_throughput_dl_kbps AS FLOAT64),
    SAFE_CAST(mean_pdcp_drb_throughput_ul_kbps AS FLOAT64),
    SAFE_CAST(enb_cfcq AS FLOAT64),
    SAFE_CAST(dl_ttis_ue_scheduled AS INT64),
    SAFE_CAST(end_earfcn_dl AS INT64),
    qci_list,
    SAFE_CAST(rrc_re_establishment_failures AS INT64),
    SAFE_CAST(rrc_re_establishment_successes AS INT64),
    SAFE_CAST(num_of_neighbors AS INT64),
    SAFE_CAST(n1_rsrp_dbm AS FLOAT64),
    SAFE_CAST(n2_rsrp_dbm AS FLOAT64),
    SAFE_CAST(n3_rsrp_dbm AS FLOAT64),
    SAFE_CAST(pusch_sinr_db AS FLOAT64),
    SAFE_CAST(ue_power_headroom_db AS FLOAT64),
    SAFE_CAST(ta_distance_meters AS FLOAT64),
    SAFE_CAST(intersite_distance_meters AS FLOAT64),
    imei,
    imsi,
    SAFE_CAST(volte_erab_attempts AS INT64),
    SAFE_CAST(volte_erab_drops AS INT64),
    volte_erab_duration ,
    SAFE_CAST(volte_erab_failed_attempts AS INT64),
    SAFE_CAST(volte_erab_release_attempts AS INT64),
    SAFE_CAST(rlc_dl_throughput_kbps AS FLOAT64),
    SAFE_CAST(rlc_pdu_dl_retransmit_rate_percentage AS FLOAT64),
    SAFE_CAST(rlc_pdu_dl_retransmitted_volume_bytes AS FLOAT64),
    SAFE_CAST(rlc_pdu_dl_volume_bytes AS FLOAT64),
    SAFE_CAST(rlc_pdu_ul_volume_bytes AS FLOAT64),
    SAFE_CAST(rlc_sdu_ul_volume_kbytes AS FLOAT64),
    SAFE_CAST(rlc_ul_throughput_kbps AS FLOAT64),
    mobile_number,
    SAFE_CAST(mean_ul_sinr_db AS FLOAT64),
    vendor,
    confidence,
    nr_serving_cell,
    nr_serving_pci,
    nr_serving_arfcn,
    en_dc_duration ,
    en_dc_duration_rate_percentage,
    maximum_number_of_carrier_components,
 average_number_of_carrier_components,
    en_dc_sgnb_addition_attempts,
    en_dc_sgnb_addition_failures,
    en_dc_sgnb_drops,
    last_5g_en_dc_release_cause,
    en_dc_downlink_volume_bytes,
    en_dc_uplink_volume_bytes,
    SAFE_CAST(time_of_last_rrc_measurement_reports_with_5g_nr_cell AS DATETIME),
    nr_last_measurement_cell,
    nr_last_measurement_pci,
    nr_last_measurement_arfcn,
    nr_rsrp_dbm,
    nr_rsrq_db,
    nr_dl_sinr_db,
    en_dc_sgnb_change_attempts,
    en_dc_sgnb_change_failures,
    en_dc_sgnb_modification_attempts,
    en_dc_sgnb_modification_failures,
    radio_type,
    call_release_cause,
    s1_u_ul_data_volume as s1_u_ul_data_volume_bytes,
    s1_u_dl_data_volume as s1_u_dl_data_volume_bytes,
    mean_uplink_ip_throughput_kbps,
    mean_downlink_ipthroughput_kbps,
    call_type,
 last_rrc_reestablishment_case,
    last_cqi,
    csl_event,
    call_segment_duration ,
    last_ue_tx_power,
    csfb,
    csfb_result,
    csfb_type,
    ue_5g_capable,
    nr_final_disposition,
    lte_carrier_aggregation_service_time ,
    nr_carrier_aggregation_service_time ,
    SAFE_CAST(intra_frequency_x2_ho_exec_attempts as INT64) AS intra_frequency_x2_ho_exec_attempts,
    SAFE_CAST(inter_frequency_x2_ho_exec_attempts as INT64) AS inter_frequency_x2_ho_exec_attempts,
    SAFE_CAST(rrc_re_establishment_attempts as INT64) AS rrc_re_establishment_attempts,
    nr_scg_failure_type,
    mean_mac_throughput_ul_kbps,
    mean_mac_throughput_dl_kbps,
    composite_call,
    fragment_id,
    global_id,
 erab_drops,
    nr_n1_pci,
    nr_n1_cell,
    nr_n1_rsrp_dbm,
    nr_n1_dl_sinr_db,
    nr_n2_pci,
    nr_n2_cell,
    nr_n2_rsrp_dbm,
    nr_n2_dl_sinr_db,
    average_number_of_nr_carrier_components,
    nr_scell_list,
    nr_scell_arfcn_list,
    lte_scell_list,
    lte_scell_arfcn_list,
    lte_carrier_aggregation_service_rate_percentage,
    erab_attempts,
    erab_failed_attempts,
    erab_normal_releases,
    erab_release_attempts,
    erab_successes,
    substring(lpad(end_enb,6,'0'),0,3) as lte_mkt,
    lpad(safe_cast(mod(safe_cast(substring(lpad(end_enb,6,'0'),0,3) as numeric),300) as STRING),3,'0') lte_base_mkt,
    mod(safe_cast(substring(lpad(band_dd, 6, '0'), 0, 3) AS numeric), 300) AS mod_val
    FROM `${truecall_src_project_id}.${truecall_src_dataset_name}.${truecall_source_tblname}`
 WHERE
    trans_dt >= CAST(trans_date as date) and arrival=arrival_dt_hr
),
data2 as (SELECT data1.*EXCEPT(mod_val),
    case
    when mod_val in (152,153,154,155,156,157,158,159,161,162,163) then 'CARTN'
    when mod_val in (120,121,122,123,124,125,126,127,180,181,182,185,186) then 'CGC'
    when mod_val in (131,132,133,134,135,136,137,138,139,140,184) then 'CTX'
    when mod_val in (142,143,144,145,146,147,148,149,151) then 'FL'
    when mod_val in (168,169,170,171,172,173,174,175,176,177) then 'GA'
    when mod_val in (188,189,190,191,192,193,195,196,197,198) then 'GP'
    when mod_val in (202,203,204,206,207,208,209,210) then 'ILWI'
    when mod_val in (224,225,226,227,228,229,230,231,232,233,234) then 'MINKY'
    when mod_val in (10,11,12,13,14,15,16,17,214,215,216,217,218,219,220) then 'MTPL'
    when mod_val in (56,57,58,59,60,61,62,64,65,66,68) then 'NE'
    when mod_val in (78,79,80,81,82,83,84,85) then 'NYM'
    when mod_val in (30,31,32,33,35,36,37,38) then 'NCAL'
    when mod_val in (0,1,2,3,5,6,7,8) then 'PNW'
    when mod_val in (241,242,243,244,245,246,247,250,251,252,253,254,255) then 'OHPA'
    when mod_val in (40,41,43,44,45,47,49,51,52,53,54) then 'SCAL'
    when mod_val in (20,21,22,23,24,25,26,27) then 'SW'
    when mod_val in (86,87,88,89,90,91,96,97,98,99,100,101,102) then 'PTS'
    when mod_val in (70,71,72,73,74) then 'UNY'
    when mod_val in (106,107,109,110,111,112,113,114,115,116,117) then 'WBV'
    else '-999' END AS submkt_new,
CASE
    WHEN duration is null OR duration =''  OR  duration ='< 0' then NULL
    when array_length(split(duration,':')) = 3 then safe_cast(split(duration,':') [offset(0)] as int64 )*3600 + safe_cast(split(duration,':') [offset(1)] as int64 )*60 + safe_cast(split(duration,':') [offset(2)] as float64 )*1
    when array_length(split(duration,':')) = 2 then safe_cast(split(duration,':') [offset(0)] as int64 )*60 + safe_cast(split(duration,':') [offset(1)] as float64 )*1
    when array_length(split(duration,':')) = 1 then safe_cast(split(duration,':') [offset(0)] as float64 )*1
    ELSE NULL
END as duration_seconds,
    CASE
    WHEN call_segment_duration is null OR call_segment_duration =''  OR  call_segment_duration ='< 0' then NULL
    when array_length(split(call_segment_duration,':')) = 3 then safe_cast(split(call_segment_duration,':') [offset(0)]as int64 )*3600 + safe_cast(split(call_segment_duration,':') [offset(1)]as int64 )*60 + safe_cast(split(call_segment_duration,':') [offset(2)]as float64 )*1
    when array_length(split(call_segment_duration,':')) = 2 then safe_cast(split(call_segment_duration,':') [offset(0)]as int64 )*60 + safe_cast(split(call_segment_duration,':') [offset(1)]as float64 )*1
    when array_length(split(call_segment_duration,':')) = 1 then safe_cast(split(call_segment_duration,':') [offset(0)]as float64 )*1
    ELSE NULL
    END as call_segment_duration_seconds,
CASE
    WHEN en_dc_duration is null OR en_dc_duration =''  OR  en_dc_duration ='< 0' then NULL
    when array_length(split(en_dc_duration,':')) = 3 then safe_cast(split(en_dc_duration,':') [offset(0)]as int64 )*3600 + safe_cast(split(en_dc_duration,':') [offset(1)]as int64 )*60 + safe_cast(split(en_dc_duration,':') [offset(2)]as float64 )*1
    when array_length(split(en_dc_duration,':')) = 2 then safe_cast(split(en_dc_duration,':') [offset(0)]as int64 )*60 + safe_cast(split(en_dc_duration,':') [offset(1)]as float64 )*1
    when array_length(split(en_dc_duration,':')) = 1 then safe_cast(split(en_dc_duration,':') [offset(0)]as float64 )*1
    ELSE NULL
    END as en_dc_duration_seconds,
CASE
    WHEN volte_erab_duration is null OR volte_erab_duration =''  OR  volte_erab_duration ='< 0' then NULL
    when array_length(split(volte_erab_duration,':')) = 3 then safe_cast(split(volte_erab_duration,':') [offset(0)]as int64 )*3600 + safe_cast(split(volte_erab_duration,':') [offset(1)]as int64 )*60 + safe_cast(split(volte_erab_duration,':') [offset(2)]as float64 )*1
    when array_length(split(volte_erab_duration,':')) = 2 then safe_cast(split(volte_erab_duration,':') [offset(0)]as int64 )*60 + safe_cast(split(volte_erab_duration,':') [offset(1)]as float64 )*1
    when array_length(split(volte_erab_duration,':')) = 1 then safe_cast(split(volte_erab_duration,':') [offset(0)]as float64 )*1
    ELSE NULL
    END as volte_erab_duration_seconds,
CASE
    WHEN last_volte_erab_duration is null OR last_volte_erab_duration ='' OR  last_volte_erab_duration ='< 0' then NULL
    when array_length(split(last_volte_erab_duration,':')) = 3 then safe_cast(split(last_volte_erab_duration,':') [offset(0)]as int64 )*3600 + safe_cast(split(last_volte_erab_duration,':') [offset(1)]as int64 )*60 + safe_cast(split(last_volte_erab_duration,':') [offset(2)]as float64 )*1
    when array_length(split(last_volte_erab_duration,':')) = 2 then safe_cast(split(last_volte_erab_duration,':') [offset(0)]as int64 )*60 + safe_cast(split(last_volte_erab_duration,':') [offset(1)]as float64 )*1
    when array_length(split(last_volte_erab_duration,':')) = 1 then safe_cast(split(last_volte_erab_duration,':') [offset(0)]as float64 )*1
    ELSE NULL
END as last_volte_erab_duration_seconds,
    `carto-os`.carto.H3_TOPARENT(`carto-os`.carto.H3_FROMGEOGPOINT(ST_GEOGPOINT(end_location_lon,end_location_lat),9), 6) as h3_06 ,
    `carto-os`.carto.H3_TOPARENT(`carto-os`.carto.H3_FROMGEOGPOINT(ST_GEOGPOINT(end_location_lon,end_location_lat),9), 7) as h3_07 ,
    `carto-os`.carto.H3_TOPARENT(`carto-os`.carto.H3_FROMGEOGPOINT(ST_GEOGPOINT(end_location_lon,end_location_lat),9), 8) as h3_08 ,
    `carto-os`.carto.H3_FROMGEOGPOINT(ST_GEOGPOINT(end_location_lon,end_location_lat),9) as h3_09,
    `${truecall_tgt_project_id}.${truecall_tgt_dataset_name}.LLtoMGRS`(end_location_lat, end_location_lon) as mgrs_1m,
    CASE WHEN LENGTH(nr_serving_cell) > 6 THEN '5G' ELSE '4G' END AS conn_tech_type,
    CASE
    WHEN LENGTH(nr_serving_cell) > 6
        THEN CASE
                WHEN nr_last_measurement_arfcn IN (0,1,677,877,2410,2560) OR nr_last_measurement_arfcn BETWEEN 174000 AND 435000 THEN 'NW'
                WHEN nr_last_measurement_arfcn BETWEEN 648000 AND 659999 THEN 'CB'
                WHEN nr_last_measurement_arfcn BETWEEN 2071000 AND 2269999 THEN 'mmW'
                ELSE 'LTE' END
        ELSE 'LTE' END AS conn_tech_type_desc,
    CASE
    WHEN LENGTH(nr_serving_cell) > 6 THEN safe_cast(safe_cast((safe_cast(RIGHT(nr_serving_cell,10) AS INTEGER)/16384) AS INTEGER) AS STRING)
        ELSE safe_cast(END_ENB AS STRING) END AS enb_gnb_id,
    CASE
        WHEN LENGTH(nr_serving_cell) > 6
        THEN CASE
                WHEN safe_cast(MOD(safe_cast(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER)>=10
                    THEN safe_cast(FLOOR(safe_cast(MOD(safe_cast(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER)/10) AS STRING)
                    ELSE safe_cast(safe_cast(MOD(safe_cast(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER) AS STRING)
                    END
                ELSE CASE
                            WHEN MOD(safe_cast(RIGHT (safe_cast(start_cell AS STRING),8) AS INTEGER),256)>=10
                                THEN safe_cast(FLOOR(MOD(safe_cast(RIGHT (safe_cast(start_cell AS STRING),8) AS INTEGER),256)/10) AS STRING)
                                ELSE safe_cast(MOD(safe_cast(RIGHT (safe_cast(start_cell AS STRING),8) AS INTEGER),256)  AS STRING)END
                    END AS start_cell_sector,
    CASE
    WHEN LENGTH(nr_serving_cell) > 6 THEN nr_last_measurement_arfcn ELSE SAFE_CAST(End_earfcn_dl AS INT) END AS channel,
    CASE
    WHEN LENGTH(nr_serving_cell) > 6
    THEN CASE
    WHEN safe_cast(MOD(SAFE_CAST(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER)>=10
    THEN safe_cast(FLOOR(SAFE_CAST(MOD(SAFE_CAST(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER)/10) AS STRING)
    ELSE safe_cast(safe_cast(MOD(SAFE_CAST(RIGHT(nr_serving_cell,10) AS INTEGER),16384) AS INTEGER) AS STRING) END
    ELSE CASE
 WHEN MOD(SAFE_CAST(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256)>=10
    THEN safe_cast(FLOOR(MOD(SAFE_CAST(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256)/10) AS STRING)
    ELSE safe_cast(MOD(SAFE_CAST(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256) AS STRING) END
    END AS end_cell_sector,
    safe_cast((MOD((CASE
    WHEN MOD(safe_cast(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256)>=10
    THEN MOD(safe_cast(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256)
    ELSE
    MOD(safe_cast(RIGHT (safe_cast(end_cell AS STRING),8) AS INTEGER),256) * 10
    END
    ),10)+1) AS STRING) AS new_carr,
    safe_cast(safe_cast(end_time as DATETIME)as DATE) as trans_dt,
    format_time("%H",extract(time from timestamp(end_time))) as hr,
    submkt,
    CAST(process_date AS datetime ) AS process_dt
from data1)
select * from data2