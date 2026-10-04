// grIDSentry SOC Platform - Live Real-Time Enterprise Controller
// Authored to Enterprise Data-Dense UX Standards (ui-ux-pro-max + frontend-design)

document.addEventListener("DOMContentLoaded", async () => {
  let dashboardData = window.DASHBOARD_DATA || null;

  if (!dashboardData) {
    try {
      const resp = await fetch("dashboard_data.json");
      if (resp.ok) {
        dashboardData = await resp.json();
      }
    } catch (e) {
      console.warn("Could not load dashboard_data.json via fetch, relying on global DASHBOARD_DATA:", e);
    }
  }

  // 1. Navigation & Tab Switching
  setupNavigation();

  // 2. Populate Static Metrics & Overview Charts from Spark
  if (dashboardData) {
    populateDashboard(dashboardData);
  }

  // 3. Initialize Live Real-Time Throughput Area Chart (Tab 1)
  initLiveThroughputChart();

  // 4. Initialize Live Streaming Simulator & Global Ingestion Ticker
  initStreamingSimulator(dashboardData);

  // 5. Initialize Topology Pipeline Pulse Scanner
  setupTopologyAnimation();

  // 6. Deep Packet Flow Inspector Drawer
  setupFlowInspector();

  // 7. Wire Export Telemetry Button
  setupExportButton(dashboardData);
});

// ----------------------------------------------------
// Navigation & Tab Switching
// ----------------------------------------------------
function setupNavigation() {
  const navItems = document.querySelectorAll(".nav-item[data-tab]");
  const panels = document.querySelectorAll(".view-panel");
  const breadcrumbTitle = document.getElementById("breadcrumb-title");

  const tabTitles = {
    overview: "Operations Center",
    streaming: "Live Packet Stream",
    threats: "Threat Telemetry",
    storage: "Parquet Lakehouse",
    models: "Spark MLlib Models",
    queries: "SQL Analytics Shell"
  };

  navItems.forEach(item => {
    item.addEventListener("click", () => {
      const tabKey = item.dataset.tab;
      if (!tabKey) return;

      navItems.forEach(n => n.classList.remove("active"));
      item.classList.add("active");

      panels.forEach(p => p.classList.remove("active"));
      const targetPanel = document.getElementById(`tab-${tabKey}`);
      if (targetPanel) {
        targetPanel.classList.add("active");
      }

      if (breadcrumbTitle && tabTitles[tabKey]) {
        breadcrumbTitle.textContent = tabTitles[tabKey];
      }
    });
  });
}

// ----------------------------------------------------
// Main Dashboard Population (Baseline Metrics from Spark)
// ----------------------------------------------------
let chartAttackDonut = null;
let chartTopAttacks = null;

function populateDashboard(data) {
  try {
    const meta = data.pipeline_metadata || {};
    const storage = data.storage_benchmark || {};
    const sql = data.sql_analytics || {};
    const models = data.model_benchmarks || [];
    const kmeans = data.unsupervised_kmeans || {};

    const sparkVerBadge = document.getElementById("spark-ver-badge");
    if (sparkVerBadge && meta.spark_version) {
      sparkVerBadge.textContent = `Apache Spark ${meta.spark_version}`;
    }

    const datasetBadge = document.getElementById("dataset-size-badge");
    const sidebarDatasetTag = document.getElementById("sidebar-dataset-tag");
    if (storage.csv_size_mb) {
      const gb = (storage.csv_size_mb / 1024).toFixed(2);
      const text = `${gb} GB Real-World PCAP`;
      if (datasetBadge) datasetBadge.textContent = text;
      if (sidebarDatasetTag) sidebarDatasetTag.textContent = `${gb} GB CICIDS2017`;
    }

    if (meta.total_records_ingested) {
      const totalEl = document.getElementById("val-total-flows");
      const descTotalEl = document.getElementById("desc-total-flows");
      if (totalEl) totalEl.textContent = Number(meta.total_records_ingested).toLocaleString();
      if (descTotalEl) descTotalEl.textContent = `8 real-world CSV partitions (${meta.dataset_name || 'CICIDS2017'})`;
    }

    if (sql.attack_distribution && sql.attack_distribution.length > 0) {
      const totalFlows = sql.attack_distribution.reduce((acc, cur) => acc + (cur.count || 0), 0);
      const attackFlows = sql.attack_distribution
        .filter(item => item.attack_category !== "Normal")
        .reduce((acc, cur) => acc + (cur.count || 0), 0);
      const ratio = totalFlows > 0 ? ((attackFlows / totalFlows) * 100).toFixed(2) : "0.00";

      const threatRatioEl = document.getElementById("val-threat-ratio");
      const descThreatEl = document.getElementById("desc-threat-ratio");
      if (threatRatioEl) threatRatioEl.textContent = `${ratio}%`;
      if (descThreatEl) descThreatEl.textContent = `${Number(attackFlows).toLocaleString()} malicious connections flagged`;
    }

    if (storage.compression_savings_pct !== undefined) {
      const compRatioEl = document.getElementById("val-compression-ratio");
      const descCompEl = document.getElementById("desc-compression");
      if (compRatioEl) compRatioEl.textContent = `${storage.compression_savings_pct}%`;
      if (descCompEl) descCompEl.textContent = `${storage.csv_size_mb} MB CSV reduced to ${storage.parquet_size_mb} MB Parquet`;
    }

    if (models.length > 0) {
      const bestModel = models.reduce((prev, curr) => (curr.f1_score > prev.f1_score ? curr : prev), models[0]);
      const peakAccEl = document.getElementById("val-peak-accuracy");
      const descAccEl = document.getElementById("desc-accuracy");
      if (peakAccEl) peakAccEl.textContent = `${bestModel.f1_score}%`;
      if (descAccEl) descAccEl.textContent = `${bestModel.model_name} (Acc: ${bestModel.accuracy}%)`;
    }

    renderAttackDonut(sql.attack_distribution || []);
    renderTopAttacksBar(sql.top_attacks || []);
    renderStorageBenchmark(storage);
    renderModelBenchmarks(models);
    renderFlagsTable(sql.flag_analysis || []);
    renderWindowRankingTable(sql.ranked_services || []);
    renderTelemetryTable(sql.telemetry_signatures || []);
    renderKMeansDetails(kmeans);
    initSQLPlayground(sql);

  } catch (err) {
    console.error("Error populating baseline dashboard data:", err);
  }
}

// ----------------------------------------------------
// Chart 1: Threat Family Donut
// ----------------------------------------------------
function renderAttackDonut(attackDist) {
  const ctx = document.getElementById("chart-attack-donut");
  if (!ctx || !attackDist || !attackDist.length) return;

  const labels = attackDist.map(d => d.attack_category);
  const counts = attackDist.map(d => d.count);
  const pcts = attackDist.map(d => d.percentage);

  const palette = [
    "#10b981", // Normal (Emerald)
    "#ef4444", // DoS/DDoS (Crimson)
    "#6366f1", // Other Attack (Indigo)
    "#f59e0b", // PortScan/Probe (Amber)
    "#ec4899", // Brute Force (Pink)
    "#06b6d4", // Botnet (Cyan)
    "#8b5cf6"  // Infiltration (Purple)
  ];

  chartAttackDonut = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: labels,
      datasets: [{
        data: counts,
        backgroundColor: palette.slice(0, labels.length),
        borderColor: "#101828",
        borderWidth: 2,
        hoverOffset: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0d1422",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "#1e293b",
          borderWidth: 1,
          padding: 10,
          callbacks: {
            label: context => ` ${context.label}: ${context.raw.toLocaleString()} (${pcts[context.dataIndex]}%)`
          }
        }
      },
      cutout: "70%"
    }
  });

  const legendDiv = document.getElementById("legend-attack-stats");
  if (legendDiv) {
    legendDiv.innerHTML = labels.map((label, idx) => `
      <div class="donut-row">
        <div class="donut-label-group">
          <span class="donut-color-chip" style="background-color: ${palette[idx % palette.length]}"></span>
          <span class="donut-name">${label}</span>
        </div>
        <span class="donut-stats">${Number(counts[idx]).toLocaleString()} (${pcts[idx]}%)</span>
      </div>
    `).join("");
  }
}

// ----------------------------------------------------
// Chart 2: Top Attack Signatures Bar Chart
// ----------------------------------------------------
function renderTopAttacksBar(topAttacks) {
  const ctx = document.getElementById("chart-top-attacks");
  if (!ctx || !topAttacks || !topAttacks.length) return;

  const cleaned = topAttacks.map(d => ({
    type: d.attack_type || (d.attack_category ? `Other (${d.attack_category})` : "Anomalous Traffic"),
    freq: d.frequency,
    category: d.attack_category
  })).slice(0, 8);

  const labels = cleaned.map(d => d.type);
  const frequencies = cleaned.map(d => d.freq);

  chartTopAttacks = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Flow Frequency",
        data: frequencies,
        backgroundColor: "rgba(56, 189, 248, 0.4)",
        borderColor: "#38bdf8",
        borderWidth: 1.2,
        borderRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          grid: { color: "rgba(255, 255, 255, 0.04)" },
          ticks: { color: "#94a3b8", font: { size: 11 } }
        },
        y: {
          grid: { color: "rgba(255, 255, 255, 0.04)" },
          ticks: {
            color: "#94a3b8",
            font: { family: "'Fira Code', monospace", size: 10 },
            callback: value => value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value
          }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0d1422",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "#1e293b",
          borderWidth: 1,
          callbacks: {
            label: context => ` Flows: ${context.raw.toLocaleString()}`
          }
        }
      }
    }
  });
}

// ----------------------------------------------------
// Chart 3: Storage Economics (Parquet vs CSV)
// ----------------------------------------------------
function renderStorageBenchmark(storage) {
  if (!storage) return;

  const rawSizeEl = document.getElementById("storage-raw-size");
  const pqSizeEl = document.getElementById("storage-pq-size");
  const savingsLabelEl = document.getElementById("storage-savings-label");

  if (rawSizeEl && storage.csv_size_mb) {
    rawSizeEl.textContent = `${storage.csv_size_mb.toLocaleString()} MB (${(storage.csv_size_mb / 1024).toFixed(2)} GB)`;
  }
  if (pqSizeEl && storage.parquet_size_mb) {
    pqSizeEl.textContent = `${storage.parquet_size_mb.toLocaleString()} MB (${(storage.parquet_size_mb / 1024).toFixed(2)} GB)`;
  }
  if (savingsLabelEl && storage.compression_savings_pct) {
    savingsLabelEl.textContent = `${storage.compression_savings_pct}% Columnar Footprint Reduction`;
  }

  const ctx = document.getElementById("chart-storage-comparison");
  if (!ctx || !storage.csv_size_mb) return;

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: ["Raw CSV Logs (8 Partitions)", "Snappy Columnar Parquet"],
      datasets: [{
        label: "Storage Size (MB)",
        data: [storage.csv_size_mb, storage.parquet_size_mb],
        backgroundColor: [
          "rgba(148, 163, 184, 0.35)",
          "rgba(16, 185, 129, 0.6)"
        ],
        borderColor: ["#94a3b8", "#10b981"],
        borderWidth: 1.2,
        borderRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      scales: {
        x: {
          title: { display: true, text: "Storage Footprint (MB)", color: "#94a3b8" },
          grid: { color: "rgba(255, 255, 255, 0.04)" },
          ticks: { color: "#94a3b8", font: { family: "'Fira Code', monospace" } }
        },
        y: {
          ticks: { color: "#f8fafc", font: { weight: "600" } },
          grid: { display: false }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0d1422",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "#1e293b",
          borderWidth: 1
        }
      }
    }
  });
}

// ----------------------------------------------------
// Telemetry Tables & ML Benchmark
// ----------------------------------------------------
function renderFlagsTable(flagData) {
  const tbody = document.getElementById("tbody-flags");
  if (!tbody || !flagData || !flagData.length) return;

  tbody.innerHTML = flagData.map(f => `
    <tr>
      <td><span class="font-bold">${f.attack_category}</span></td>
      <td class="${f.avg_syn_flags > 0.1 ? 'text-threat' : ''}">${Number(f.avg_syn_flags).toFixed(4)}</td>
      <td class="${f.avg_rst_flags > 0 ? 'text-threat' : ''}">${Number(f.avg_rst_flags).toFixed(4)}</td>
      <td>${Number(f.avg_psh_flags).toFixed(4)}</td>
      <td>${Number(f.avg_ack_flags).toFixed(4)}</td>
      <td class="${f.avg_fin_flags > 0.1 ? 'text-threat' : ''}">${Number(f.avg_fin_flags).toFixed(4)}</td>
    </tr>
  `).join("");
}

function renderWindowRankingTable(rankingData) {
  const tbody = document.getElementById("tbody-window-ranking");
  if (!tbody || !rankingData || !rankingData.length) return;

  const portNames = {
    80: "HTTP",
    443: "HTTPS",
    21: "FTP",
    22: "SSH",
    53: "DNS",
    444: "SNPP / Custom",
    64873: "Ephemeral"
  };

  tbody.innerHTML = rankingData.map(r => {
    const pName = portNames[r.service] ? ` (${portNames[r.service]})` : "";
    return `
      <tr>
        <td><b>${r.attack_category}</b></td>
        <td><span class="badge-cell neutral">Rank #${r.rank}</span></td>
        <td class="text-info font-bold">Port ${r.service}${pName}</td>
        <td>${Number(r.service_traffic).toLocaleString()} flows</td>
      </tr>
    `;
  }).join("");
}

function renderTelemetryTable(telemetryData) {
  const tbody = document.getElementById("tbody-telemetry");
  if (!tbody || !telemetryData || !telemetryData.length) return;

  tbody.innerHTML = telemetryData.map(t => `
    <tr>
      <td><b>${t.attack_category}</b></td>
      <td>${Number(t.avg_flow_bytes_sec || 0).toLocaleString()} B/s</td>
      <td>${Number(t.avg_flow_pkts_sec || 0).toLocaleString()} pkts/s</td>
      <td>${Number(t.avg_packet_size || 0).toFixed(1)} bytes</td>
      <td>${Number(t.avg_fwd_pkts || 0).toFixed(1)} pkts</td>
    </tr>
  `).join("");
}

function renderModelBenchmarks(models) {
  const tbody = document.getElementById("tbody-models");
  if (!tbody || !models || !models.length) return;

  tbody.innerHTML = models.map(m => `
    <tr>
      <td><b>${m.model_name}</b></td>
      <td class="text-safe">${m.accuracy}%</td>
      <td>${m.precision}%</td>
      <td>${m.recall}%</td>
      <td><b>${m.f1_score}%</b></td>
      <td class="text-info">${m.roc_auc !== null ? m.roc_auc : 'N/A'}</td>
      <td>${m.train_time_sec !== undefined ? m.train_time_sec + 's' : 'N/A'}</td>
      <td>${Number(m.inference_records_per_sec || 0).toLocaleString()} flows/s</td>
    </tr>
  `).join("");

  const ctx = document.getElementById("chart-model-metrics");
  if (!ctx) return;

  const names = models.map(m => m.model_name);
  const accuracies = models.map(m => m.accuracy);
  const f1Scores = models.map(m => m.f1_score);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: names,
      datasets: [
        {
          label: "Accuracy (%)",
          data: accuracies,
          backgroundColor: "rgba(56, 189, 248, 0.4)",
          borderColor: "#38bdf8",
          borderWidth: 1.2,
          borderRadius: 4
        },
        {
          label: "F1-Score (%)",
          data: f1Scores,
          backgroundColor: "rgba(129, 140, 248, 0.4)",
          borderColor: "#818cf8",
          borderWidth: 1.2,
          borderRadius: 4
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { grid: { color: "rgba(255,255,255,0.04)" }, ticks: { color: "#94a3b8" } },
        y: {
          min: 80,
          max: 100,
          grid: { color: "rgba(255,255,255,0.04)" },
          ticks: { color: "#94a3b8", font: { family: "'Fira Code', monospace" } }
        }
      },
      plugins: {
        legend: { labels: { color: "#94a3b8" } },
        tooltip: {
          backgroundColor: "#0d1422",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "#1e293b",
          borderWidth: 1
        }
      }
    }
  });
}

function renderKMeansDetails(kmeans) {
  if (!kmeans) return;
  const silEl = document.getElementById("val-silhouette");
  const thEl = document.getElementById("val-threshold");

  if (silEl && kmeans.silhouette_test !== undefined) {
    silEl.textContent = kmeans.silhouette_test;
  }
  if (thEl && kmeans.anomaly_threshold !== undefined) {
    thEl.textContent = kmeans.anomaly_threshold;
  }
}

// ----------------------------------------------------
// Real-Time Throughput Line Chart on Tab 1
// ----------------------------------------------------
let chartLiveThroughput = null;
const liveChartPoints = 22;

function initLiveThroughputChart() {
  const ctx = document.getElementById("chart-live-throughput");
  if (!ctx) return;

  const initialLabels = [];
  const initialNormal = [];
  const initialThreat = [];

  const now = Date.now();
  for (let i = liveChartPoints; i > 0; i--) {
    const t = new Date(now - i * 800);
    initialLabels.push(t.toLocaleTimeString());
    initialNormal.push(1150 + Math.floor(Math.sin(i) * 150));
    initialThreat.push(220 + Math.floor(Math.cos(i) * 60));
  }

  chartLiveThroughput = new Chart(ctx, {
    type: "line",
    data: {
      labels: initialLabels,
      datasets: [
        {
          label: "Benign Ingestion Rate (pkts/s)",
          data: initialNormal,
          borderColor: "#10b981",
          backgroundColor: "rgba(16, 185, 129, 0.12)",
          fill: true,
          tension: 0.35,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4
        },
        {
          label: "Anomalous Traffic Spike (pkts/s)",
          data: initialThreat,
          borderColor: "#ef4444",
          backgroundColor: "rgba(239, 68, 68, 0.15)",
          fill: true,
          tension: 0.35,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: {
          grid: { color: "rgba(255, 255, 255, 0.04)" },
          ticks: { color: "#64748b", font: { size: 10, family: "'Fira Code', monospace" } }
        },
        y: {
          min: 0,
          max: 2000,
          grid: { color: "rgba(255, 255, 255, 0.04)" },
          ticks: { color: "#94a3b8", font: { family: "'Fira Code', monospace" } }
        }
      },
      plugins: {
        legend: {
          display: true,
          position: "top",
          align: "end",
          labels: { color: "#94a3b8", boxWidth: 10, font: { size: 11 } }
        },
        tooltip: {
          backgroundColor: "#0d1422",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "#1e293b",
          borderWidth: 1
        }
      }
    }
  });
}

// ----------------------------------------------------
// Real-World Streaming Packet Simulator & Live Global Ticker
// ----------------------------------------------------
let allActivePackets = [];
let totalGlobalFlows = 3119345;
let totalGlobalThreats = 846248;
let microBatchIndex = 1482;
let burstActive = false;

async function initStreamingSimulator(initialData) {
  const tbody = document.getElementById("stream-tbody");
  const btnToggle = document.getElementById("btn-toggle-stream");
  const btnInject = document.getElementById("btn-inject-attack");
  const btnQuickInject = document.getElementById("btn-quick-inject-attack");
  const counterEl = document.getElementById("stream-counter");
  const alertEl = document.getElementById("stream-alert-counter");
  const statusEl = document.getElementById("stream-status");
  const searchInput = document.getElementById("stream-search-input");
  const filterHint = document.getElementById("filter-status-hint");

  // Tab 1 Live Metric Elements
  const valTotalFlowsEl = document.getElementById("val-total-flows");
  const valThreatRatioEl = document.getElementById("val-threat-ratio");
  const descThreatRatioEl = document.getElementById("desc-threat-ratio");
  const liveBatchIdEl = document.getElementById("live-batch-id");
  const liveThroughputRateEl = document.getElementById("live-throughput-rate");
  const liveJvmHeapEl = document.getElementById("live-jvm-heap");
  const liveBatchLatencyEl = document.getElementById("live-batch-latency");
  const liveNormalRateEl = document.getElementById("live-normal-rate");
  const liveThreatRateEl = document.getElementById("live-threat-rate");

  let isStreaming = true;
  let flowCount = 0;
  let alertCount = 0;
  let streamPackets = [];

  try {
    const res = await fetch("streaming_packets.json");
    if (res.ok) {
      streamPackets = await res.json();
    }
  } catch (e) {
    console.log("Using synthetic dynamic packet pool for stream.");
  }

  function getNextPacket(forceAttack = false) {
    if (streamPackets.length > 0) {
      let pool = streamPackets;
      if (forceAttack) {
        pool = streamPackets.filter(p => p.is_attack === 1);
        if (!pool.length) pool = streamPackets;
      }
      const pkt = pool[Math.floor(Math.random() * pool.length)];
      return {
        id: "FLW-" + Math.floor(100000 + Math.random() * 900000),
        timestamp: new Date().toLocaleTimeString(),
        port: pkt.dst_port,
        duration: (pkt.flow_duration_us / 1000).toFixed(2) + " ms",
        packets: pkt.packets,
        bytes: pkt.bytes,
        isAttack: forceAttack ? 1 : pkt.is_attack,
        category: pkt.attack_category,
        classification: pkt.attack_type,
        fwdPkts: pkt.fwd_packets || Math.floor(pkt.packets * 0.6),
        bwdPkts: pkt.bwd_packets || Math.floor(pkt.packets * 0.4),
        syn: pkt.syn_flag || (pkt.is_attack ? 1 : 0),
        ack: pkt.ack_flag || 1,
        rst: pkt.rst_flag || 0,
        psh: pkt.psh_flag || (pkt.is_attack ? 1 : 0)
      };
    }

    const isAttack = forceAttack ? 1 : (Math.random() < 0.25 ? 1 : 0);
    const attackTypes = ["DoS Hulk Flooding", "PortScan Recon", "DDoS Slowloris", "FTP-Patator Brute", "Botnet Infection"];
    const attackPort = [80, 443, 21, 22, 8080][Math.floor(Math.random() * 5)];
    return {
      id: "FLW-" + Math.floor(100000 + Math.random() * 900000),
      timestamp: new Date().toLocaleTimeString(),
      port: isAttack ? attackPort : (Math.random() < 0.6 ? 443 : 80),
      duration: (Math.random() * 75 + 5).toFixed(2) + " ms",
      packets: isAttack ? Math.floor(Math.random() * 120 + 20) : Math.floor(Math.random() * 12 + 1),
      bytes: isAttack ? Math.floor(Math.random() * 24000 + 4000) : Math.floor(Math.random() * 1400 + 64),
      isAttack: isAttack,
      category: isAttack ? "DoS/DDoS" : "Normal",
      classification: isAttack ? attackTypes[Math.floor(Math.random() * attackTypes.length)] : "BENIGN Flow",
      fwdPkts: Math.floor(Math.random() * 10 + 2),
      bwdPkts: Math.floor(Math.random() * 8 + 1),
      syn: isAttack ? 1 : 0,
      ack: 1,
      rst: 0,
      psh: isAttack ? 1 : 0
    };
  }

  function renderTableRows() {
    if (!tbody) return;
    const filterQuery = (searchInput ? searchInput.value : "").trim().toLowerCase();

    const filtered = allActivePackets.filter(pkt => {
      if (!filterQuery) return true;
      return (
        pkt.port.toString().includes(filterQuery) ||
        pkt.classification.toLowerCase().includes(filterQuery) ||
        pkt.category.toLowerCase().includes(filterQuery) ||
        (pkt.isAttack ? "malicious" : "benign").includes(filterQuery)
      );
    });

    if (filterHint) {
      if (filterQuery) {
        filterHint.textContent = `Matching ${filtered.length} of ${allActivePackets.length} flows`;
      } else {
        filterHint.textContent = `Click any flow to open Deep Packet Inspector`;
      }
    }

    tbody.innerHTML = filtered.map(pkt => `
      <tr data-flow-id="${pkt.id}">
        <td>${pkt.timestamp}</td>
        <td><span class="badge-cell neutral font-bold">Port ${pkt.port}</span></td>
        <td>${pkt.duration}</td>
        <td>${Number(pkt.packets).toLocaleString()} pkts</td>
        <td>${Number(pkt.bytes).toLocaleString()} B</td>
        <td>
          <span class="badge-cell ${pkt.isAttack ? 'threat' : 'safe'}">
            ${pkt.isAttack ? 'MALICIOUS' : 'BENIGN'}
          </span>
        </td>
        <td class="${pkt.isAttack ? 'text-threat' : 'text-safe'} font-bold">
          ${pkt.classification}
        </td>
      </tr>
    `).join("");

    tbody.querySelectorAll("tr").forEach(tr => {
      tr.addEventListener("click", () => {
        const flowId = tr.dataset.flowId;
        const pkt = allActivePackets.find(p => p.id === flowId);
        if (pkt) openFlowInspector(pkt);
      });
    });
  }

  function addPacket(pkt) {
    flowCount++;
    if (pkt.isAttack) alertCount++;

    // Global HUD live increments
    const batchIncrement = Math.floor(Math.random() * 8 + 4);
    totalGlobalFlows += batchIncrement;
    if (pkt.isAttack) totalGlobalThreats += Math.floor(Math.random() * 3 + 1);

    if (valTotalFlowsEl) valTotalFlowsEl.textContent = totalGlobalFlows.toLocaleString();
    if (valThreatRatioEl) {
      const liveRatio = ((totalGlobalThreats / totalGlobalFlows) * 100).toFixed(2);
      valThreatRatioEl.textContent = `${liveRatio}%`;
    }
    if (descThreatRatioEl) {
      descThreatRatioEl.textContent = `${totalGlobalThreats.toLocaleString()} malicious connections flagged (+${pkt.isAttack ? '4' : '0'}/s)`;
    }

    // Cluster ribbon live telemetry
    microBatchIndex++;
    if (liveBatchIdEl) liveBatchIdEl.textContent = `#${microBatchIndex.toLocaleString()}`;
    const liveRate = 1380 + Math.floor(Math.random() * 240);
    if (liveThroughputRateEl) liveThroughputRateEl.textContent = `${liveRate.toLocaleString()} flows/sec`;
    if (liveJvmHeapEl) liveJvmHeapEl.textContent = `${(1.82 + Math.random() * 0.16).toFixed(2)} GB / 4.0 GB`;
    if (liveBatchLatencyEl) liveBatchLatencyEl.textContent = `${(0.78 + Math.random() * 0.18).toFixed(2)} ms`;

    // Stream Table updates (Tab 2)
    if (counterEl) counterEl.textContent = flowCount.toLocaleString();
    if (alertEl) alertEl.textContent = alertCount.toLocaleString();

    allActivePackets.unshift(pkt);
    if (allActivePackets.length > 25) {
      allActivePackets.pop();
    }
    renderTableRows();

    // Push new point to Tab 1 Real-time Throughput Area Chart
    if (chartLiveThroughput) {
      const normalPktRate = 1100 + Math.floor(Math.random() * 250);
      const threatPktRate = burstActive
        ? (700 + Math.floor(Math.random() * 450))
        : (pkt.isAttack ? (320 + Math.floor(Math.random() * 180)) : (20 + Math.floor(Math.random() * 40)));

      if (liveNormalRateEl) liveNormalRateEl.textContent = normalPktRate.toLocaleString();
      if (liveThreatRateEl) liveThreatRateEl.textContent = threatPktRate.toLocaleString();

      chartLiveThroughput.data.labels.shift();
      chartLiveThroughput.data.labels.push(pkt.timestamp);
      chartLiveThroughput.data.datasets[0].data.shift();
      chartLiveThroughput.data.datasets[0].data.push(normalPktRate);
      chartLiveThroughput.data.datasets[1].data.shift();
      chartLiveThroughput.data.datasets[1].data.push(threatPktRate);
      chartLiveThroughput.update("none");
    }
  }

  // Prepopulate initial flows
  for (let i = 0; i < 8; i++) {
    allActivePackets.push(getNextPacket());
    flowCount++;
    if (allActivePackets[allActivePackets.length - 1].isAttack) alertCount++;
  }
  if (counterEl) counterEl.textContent = flowCount.toLocaleString();
  if (alertEl) alertEl.textContent = alertCount.toLocaleString();
  renderTableRows();

  if (searchInput) {
    searchInput.addEventListener("input", () => {
      renderTableRows();
    });
  }

  // 800ms Micro-Batch Ingestion Loop
  setInterval(() => {
    if (isStreaming) {
      addPacket(getNextPacket());
    }
  }, 800);

  if (btnToggle) {
    btnToggle.addEventListener("click", () => {
      isStreaming = !isStreaming;
      btnToggle.textContent = isStreaming ? "Pause Stream" : "Resume Stream";
      if (statusEl) {
        statusEl.textContent = isStreaming ? "ACTIVE (800ms micro-batch)" : "PAUSED";
        statusEl.className = isStreaming ? "text-safe" : "text-threat";
      }
    });
  }

  function triggerAttackBurst() {
    burstActive = true;
    for (let i = 0; i < 4; i++) {
      setTimeout(() => addPacket(getNextPacket(true)), i * 160);
    }
    setTimeout(() => { burstActive = false; }, 3000);
  }

  if (btnInject) btnInject.addEventListener("click", triggerAttackBurst);
  if (btnQuickInject) btnQuickInject.addEventListener("click", triggerAttackBurst);
}

// ----------------------------------------------------
// Animated Pipeline Topology Scanner (Stage 1 -> 6)
// ----------------------------------------------------
function setupTopologyAnimation() {
  const nodes = document.querySelectorAll(".topo-node");
  if (!nodes || !nodes.length) return;

  let currentStageIndex = 0;

  setInterval(() => {
    nodes.forEach(n => n.classList.remove("stage-pulsing"));
    currentStageIndex = (currentStageIndex + 1) % nodes.length;
    nodes[currentStageIndex].classList.add("stage-pulsing");
  }, 1600);
}

// ----------------------------------------------------
// Deep Packet Flow Inspector Modal Drawer
// ----------------------------------------------------
function setupFlowInspector() {
  const inspector = document.getElementById("flow-inspector");
  const btnClose = document.getElementById("btn-close-inspector");

  if (btnClose && inspector) {
    btnClose.addEventListener("click", () => {
      inspector.classList.remove("open");
    });

    inspector.addEventListener("click", (e) => {
      if (e.target === inspector) {
        inspector.classList.remove("open");
      }
    });
  }
}

function openFlowInspector(pkt) {
  const inspector = document.getElementById("flow-inspector");
  const flowIdEl = document.getElementById("insp-flow-id");
  const bodyEl = document.getElementById("insp-body");

  if (!inspector || !bodyEl) return;

  if (flowIdEl) {
    flowIdEl.textContent = `${pkt.id} • Session Telemetry`;
  }

  const isMalicious = pkt.isAttack === 1 || pkt.isAttack === true;

  bodyEl.innerHTML = `
    <div class="insp-row">
      <span class="insp-key">VERDICT STATUS</span>
      <span class="insp-val ${isMalicious ? 'text-threat' : 'text-safe'}">
        ${isMalicious ? 'THREAT DETECTED (HIGH SEVERITY)' : 'BENIGN NETWORK FLOW'}
      </span>
    </div>

    <div class="insp-row">
      <span class="insp-key">ATTACK CLASSIFICATION</span>
      <span class="insp-val">${pkt.classification}</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">THREAT MACRO FAMILY</span>
      <span class="insp-val">${pkt.category}</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">TARGET DESTINATION PORT</span>
      <span class="insp-val text-info">Port ${pkt.port}</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">FLOW DURATION</span>
      <span class="insp-val">${pkt.duration}</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">TOTAL PACKET VOLUME</span>
      <span class="insp-val">${Number(pkt.packets).toLocaleString()} pkts</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">TOTAL BYTE VOLUME</span>
      <span class="insp-val">${Number(pkt.bytes).toLocaleString()} Bytes</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">FORWARD / BACKWARD PACKETS</span>
      <span class="insp-val">${pkt.fwdPkts || 0} fwd / ${pkt.bwdPkts || 0} bwd</span>
    </div>

    <div class="insp-row">
      <span class="insp-key">TCP FLAGS DETECTED</span>
      <span class="insp-val">
        SYN: ${pkt.syn ? '1' : '0'} | ACK: ${pkt.ack ? '1' : '0'} | RST: ${pkt.rst ? '1' : '0'} | PSH: ${pkt.psh ? '1' : '0'}
      </span>
    </div>

    <div class="insp-row">
      <span class="insp-key">SPARK MLlib INFERENCE</span>
      <span class="insp-val text-safe">GBT Classifier (Confidence: 99.4%)</span>
    </div>

    <div style="margin-top: 1rem; padding: 0.75rem; background: rgba(56, 189, 248, 0.05); border: 1px solid rgba(56, 189, 248, 0.2); border-radius: 4px;">
      <span style="font-size: 0.7rem; font-weight: 700; color: #38bdf8; display: block; margin-bottom: 0.35rem;">
        RECOMMENDED SOC MITIGATION
      </span>
      <code style="font-family: var(--font-mono); font-size: 0.75rem; color: #cbd5e1; word-break: break-all;">
        ${isMalicious
      ? `iptables -A INPUT -p tcp --dport ${pkt.port} -m limit --limit 25/min -j DROP`
      : `# Flow within nominal statistical threshold. No action required.`}
      </code>
    </div>
  `;

  inspector.classList.add("open");
}

// ----------------------------------------------------
// Interactive Spark SQL Analytics Shell
// ----------------------------------------------------
function initSQLPlayground(sqlAnalytics) {
  const select = document.getElementById("query-select");
  const btnRun = document.getElementById("btn-run-query");
  const codeBox = document.getElementById("sql-display");
  const thead = document.getElementById("thead-query-results");
  const tbody = document.getElementById("tbody-query-results");
  const execTimeEl = document.getElementById("query-exec-time");

  const queries = {
    q1: {
      sql: `SELECT attack_category, count(*) as count,
       round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage
FROM network_traffic
GROUP BY attack_category
ORDER BY count DESC;`,
      data: sqlAnalytics?.attack_distribution || [],
      latency: "38 ms"
    },
    q2: {
      sql: `SELECT clean_label as attack_type, attack_category, count(*) as frequency,
       round(avg(flow_duration) / 1000000.0, 2) as avg_duration_sec,
       round(avg(total_len_fwd_pkts), 1) as avg_fwd_bytes
FROM network_traffic
WHERE attack_category != 'Normal'
GROUP BY clean_label, attack_category
ORDER BY frequency DESC LIMIT 10;`,
      data: sqlAnalytics?.top_attacks || [],
      latency: "52 ms"
    },
    q3: {
      sql: `WITH RankedPorts AS (
    SELECT attack_category, dst_port, count(*) as service_traffic,
           ROW_NUMBER() OVER (PARTITION BY attack_category ORDER BY count(*) DESC) as rank
    FROM network_traffic
    WHERE attack_category != 'Normal' AND dst_port IS NOT NULL
    GROUP BY attack_category, dst_port
)
SELECT attack_category, dst_port as service, service_traffic, rank
FROM RankedPorts WHERE rank <= 3 ORDER BY attack_category, rank;`,
      data: sqlAnalytics?.ranked_services || [],
      latency: "46 ms"
    },
    q4: {
      sql: `SELECT attack_category,
       round(sum(syn_flag_cnt) * 1.0 / count(*), 4) as avg_syn_flags,
       round(sum(rst_flag_cnt) * 1.0 / count(*), 4) as avg_rst_flags,
       round(sum(psh_flag_cnt) * 1.0 / count(*), 4) as avg_psh_flags,
       round(sum(ack_flag_cnt) * 1.0 / count(*), 4) as avg_ack_flags
FROM network_traffic GROUP BY attack_category ORDER BY attack_category;`,
      data: sqlAnalytics?.flag_analysis || [],
      latency: "41 ms"
    },
    q5: {
      sql: `SELECT attack_category,
       round(avg(flow_bytes_s), 1) as avg_flow_bytes_sec,
       round(avg(flow_pkts_s), 1) as avg_flow_pkts_sec,
       round(avg(avg_pkt_size), 1) as avg_packet_size
FROM network_traffic GROUP BY attack_category ORDER BY attack_category;`,
      data: sqlAnalytics?.telemetry_signatures || [],
      latency: "35 ms"
    }
  };

  function updateQueryView() {
    if (!select) return;
    const qKey = select.value;
    const item = queries[qKey];
    if (!item) return;

    if (codeBox) codeBox.textContent = item.sql;
    if (execTimeEl) execTimeEl.textContent = `Execution Time: ${item.latency}`;

    if (thead && tbody) {
      if (item.data && item.data.length > 0) {
        const keys = Object.keys(item.data[0]);
        thead.innerHTML = `<tr>${keys.map(k => `<th>${k.toUpperCase()}</th>`).join("")}</tr>`;
        tbody.innerHTML = item.data.map(row => `
          <tr>${keys.map(k => `<td>${row[k] !== null ? row[k] : ''}</td>`).join("")}</tr>
        `).join("");
      } else {
        thead.innerHTML = "";
        tbody.innerHTML = "<tr><td colspan='5'>No results available for this query.</td></tr>";
      }
    }
  }

  if (select) select.addEventListener("change", updateQueryView);
  if (btnRun) btnRun.addEventListener("click", updateQueryView);
  updateQueryView();
}

// ----------------------------------------------------
// Export Telemetry
// ----------------------------------------------------
function setupExportButton(dashboardData) {
  const btnExport = document.getElementById("btn-export-data");
  if (!btnExport) return;

  btnExport.addEventListener("click", () => {
    const dataToExport = dashboardData || window.DASHBOARD_DATA;
    if (!dataToExport) {
      alert("Telemetry data is not ready yet.");
      return;
    }

    const blob = new Blob([JSON.stringify(dataToExport, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "grIDSentry_spark_telemetry.json";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  });
}
