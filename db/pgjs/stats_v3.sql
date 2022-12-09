SELECT t1.date_id::varchar(10) as time,
       'Network'               as object,
       "Cell Availability",
       "Call Setup Success Rate",
       "RRC Setup Success Rate (Service) (%)",
       "RRC Setup Success Rate (Signaling) (%)",
       "E-RAB Setup Success Rate_non-GBR (%)",
       "E-RAB Setup Success Rate (%)",
       "Erab Drop Call rate",
       "Handover In Success Rate",
       "UL BLER",
       "DL User Throughput",
       "UL User Throughput",
       "DL Cell Throughput",
       "UL Cell Throughput",
       "DL Data Volume",
       "UL Data Volume",
       "Max of RRC Connected User",
       "Max of Active User",
       "Packet Loss (DL)",
       "Packet Loss UL",
       "Latency (only Radio interface)",
       "DL QPSK %",
       "DL 16QAM%",
       "DL 64QAM%",
       "DL 256QAM%",
       "UL QPSK %",
       "UL 16QAM%",
       "UL 64QAM%",
       "UL 256QAM%",
       "Resource Block Utilizing Rate (DL)",
       "Resource Block Utilizing Rate (UL)",
       "Average CQI",
       "Average RSSI",
       "Intrafreq HOSR",
       "VoLTE Redirection Success Rate",
       "Interfreq HOSR"
FROM stats_v3.eutrancellfdd_std_kpi_view as t1
         LEFT JOIN stats_v3.eutrancellfdd_v_std_kpi_view as t2
                   USING (date_id, "Region", "Cluster_ID")
         LEFT JOIN stats_v3.eutrancellfddflex_std_kpi_view as t3
                   USING (date_id, "Region", "Cluster_ID")
         LEFT JOIN stats_v3.eutrancellrelation_std_kpi_view as t4
                   USING (date_id, "Region", "Cluster_ID")
WHERE t1."Region" = 'All';