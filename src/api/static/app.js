/**
 * Frontend Application for Fake News Classification Pipeline
 * Handles SSE streaming, interactive fact-checking, HDFS explorer, and model metrics.
 */

// State
let streamEvents = [];
let isSimulatorRunning = false;
let sseSource = null;

const SAMPLES = {
  reliable: {
    title: "Reuters: U.S. Senate passes bipartisan $1 trillion infrastructure bill",
    text: "WASHINGTON (Reuters) - The U.S. Senate on Friday approved a sweeping $1 trillion bipartisan infrastructure bill to rebuild the nation's roads, bridges, and broadband networks. The legislation passed with broad support from both Democrats and Republicans after months of negotiations with the White House. Lawmakers cited the urgent need to modernize transportation networks and clean energy grids across the country."
  },
  unreliable: {
    title: "BREAKING BOMBSHELL: Secret Video Proves Shadow Cabal Controlled Weather Systems!!",
    text: "Shocking leaked recordings from whistleblower insiders reveal a dark underground conspiracy manipulating the global climate with hidden laser arrays! Mainstream media is frantically covering up this viral video evidence. You won't believe what happened when top scientists tried to warn the public before they were silenced!"
  }
};

document.addEventListener("DOMContentLoaded", () => {
  initHealthCheck();
  initSSE();
  initForm();
  loadMetrics();
  loadHDFSFiles();

  // Poll health and metrics periodically
  setInterval(initHealthCheck, 5000);
  setInterval(loadMetrics, 10000);
});

// Health & System Status
async function initHealthCheck() {
  try {
    const res = await fetch("/api/health");
    if (!res.ok) return;
    const data = await res.json();

    updateStatusPill("status-kafka", data.services.kafka_connected);
    updateStatusPill("status-hdfs", data.services.hdfs_connected);
    updateStatusPill("status-model", data.services.model_ready);

    isSimulatorRunning = data.services.simulator_running;
    updateSimulatorBtnState();
  } catch (err) {
    console.warn("Health check error:", err);
  }
}

function updateStatusPill(elemId, isOnline) {
  const el = document.getElementById(elemId);
  if (!el) return;
  el.classList.remove("online", "offline");
  el.classList.add(isOnline ? "online" : "offline");
}

function updateSimulatorBtnState() {
  const btn = document.getElementById("btn-toggle-sim");
  const txt = document.getElementById("sim-btn-text");
  if (!btn || !txt) return;

  if (isSimulatorRunning) {
    btn.classList.add("running");
    txt.textContent = "Stop Simulator";
  } else {
    btn.classList.remove("running");
    txt.textContent = "Start Simulator";
  }
}

// Simulator Toggle
document.getElementById("btn-toggle-sim")?.addEventListener("click", async () => {
  try {
    if (isSimulatorRunning) {
      await fetch("/api/simulator/stop", { method: "POST" });
      isSimulatorRunning = false;
    } else {
      await fetch("/api/simulator/start?interval=2.5", { method: "POST" });
      isSimulatorRunning = true;
    }
    updateSimulatorBtnState();
  } catch (err) {
    console.error("Simulator toggle failed:", err);
  }
});

// SSE Real-time Feed
function initSSE() {
  if (sseSource) sseSource.close();
  sseSource = new EventSource("/api/stream");

  sseSource.addEventListener("classified_article", (e) => {
    try {
      const article = JSON.parse(e.data);
      addStreamArticle(article);
    } catch (err) {
      console.error("Error parsing stream article:", err);
    }
  });

  sseSource.onerror = () => {
    // Reconnects automatically
  };
}

function addStreamArticle(article) {
  streamEvents.unshift(article);
  if (streamEvents.length > 150) streamEvents.pop();

  renderStreamTable();
  updateStatsCards();
}

function renderStreamTable() {
  const tbody = document.getElementById("stream-tbody");
  const counter = document.getElementById("stream-counter");
  if (!tbody) return;

  if (counter) counter.textContent = `${streamEvents.length} events`;

  if (streamEvents.length === 0) {
    tbody.innerHTML = `
      <tr class="empty-row">
        <td colspan="5">
          <div class="empty-state">
            <span class="spinner"></span>
            <p>Waiting for articles from Kafka stream... Click <strong>Start Simulator</strong> or submit an article above!</p>
          </div>
        </td>
      </tr>`;
    return;
  }

  tbody.innerHTML = streamEvents.map((art, idx) => {
    const isRel = art.classification?.is_reliable;
    const label = art.classification?.label?.toUpperCase() || (isRel ? "RELIABLE" : "UNRELIABLE");
    const badgeClass = isRel ? "badge-reliable" : "badge-unreliable";
    const conf = Math.round((art.classification?.confidence || 0.5) * 100);
    const time = art.processed_at ? art.processed_at.split("T")[1]?.slice(0, 8) : "--:--:--";

    return `
      <tr onclick="openArticleModal(${idx})">
        <td class="time-cell">${time}</td>
        <td><div class="stream-headline">${escapeHtml(art.title)}</div></td>
        <td><span class="badge ${badgeClass}">${label}</span></td>
        <td><strong>${conf}%</strong></td>
        <td><span class="badge badge-subtle">${escapeHtml(art.source || "stream")}</span></td>
      </tr>
    `;
  }).join("");
}

function clearStream() {
  streamEvents = [];
  renderStreamTable();
  updateStatsCards();
}

function updateStatsCards() {
  const processedEl = document.getElementById("val-processed");
  const ratioEl = document.getElementById("val-reliable-ratio");
  const countsEl = document.getElementById("val-reliable-counts");

  const total = streamEvents.length;
  const reliable = streamEvents.filter(e => e.classification?.is_reliable).length;
  const ratio = total > 0 ? Math.round((reliable / total) * 100) : 0;

  if (processedEl) processedEl.textContent = total;
  if (ratioEl) ratioEl.textContent = `${ratio}%`;
  if (countsEl) countsEl.textContent = `${reliable} reliable / ${total} total`;
}

// Sandbox Form & Prediction
function initForm() {
  const form = document.getElementById("verify-form");
  const btnIngest = document.getElementById("btn-ingest");

  form?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const title = document.getElementById("input-title").value.trim();
    const text = document.getElementById("input-text").value.trim();
    if (!title || !text) return;

    try {
      const res = await fetch("/api/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title, text })
      });
      const data = await res.json();
      displayPrediction(data);
    } catch (err) {
      alert("Classification failed: " + err);
    }
  });

  btnIngest?.addEventListener("click", async () => {
    const title = document.getElementById("input-title").value.trim();
    const text = document.getElementById("input-text").value.trim();
    if (!title || !text) {
      alert("Please provide both headline and body text.");
      return;
    }

    try {
      btnIngest.disabled = true;
      btnIngest.innerHTML = "<span>Publishing to Kafka...</span>";

      const res = await fetch("/api/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title, text, source: "sandbox_ui" })
      });
      const data = await res.json();

      if (data.classification) {
        displayPrediction(data.classification);
      }

      btnIngest.disabled = false;
      btnIngest.innerHTML = `
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M22 2 11 13"></path>
          <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
        </svg>
        <span>Ingest to Kafka Pipeline</span>
      `;
    } catch (err) {
      btnIngest.disabled = false;
      alert("Ingestion failed: " + err);
    }
  });
}

function displayPrediction(pred) {
  const box = document.getElementById("prediction-result");
  const badge = document.getElementById("res-badge");
  const confText = document.getElementById("res-confidence");
  const bar = document.getElementById("res-bar");
  const tokensContainer = document.getElementById("res-tokens");
  if (!box) return;

  box.classList.remove("hidden");

  const isRel = pred.is_reliable;
  const confPct = Math.round((pred.confidence || 0.5) * 100);

  badge.className = `label-badge ${isRel ? 'reliable' : 'unreliable'}`;
  badge.textContent = isRel ? "RELIABLE NEWS" : "UNRELIABLE / FAKE NEWS";

  confText.textContent = `${confPct}%`;
  bar.className = `confidence-bar ${isRel ? '' : 'unreliable'}`;
  bar.style.width = `${confPct}%`;

  if (tokensContainer) {
    const signals = pred.top_signals || [];
    if (signals.length === 0) {
      tokensContainer.innerHTML = "<span class='text-dim'>No strong vocabulary triggers found.</span>";
    } else {
      tokensContainer.innerHTML = signals.map(s => `
        <span class="token-chip ${s.indication}">
          <strong>${escapeHtml(s.word)}</strong> (${s.impact > 0 ? '+' : ''}${s.impact})
        </span>
      `).join("");
    }
  }
}

function loadSample(type) {
  const s = SAMPLES[type];
  if (!s) return;
  document.getElementById("input-title").value = s.title;
  document.getElementById("input-text").value = s.text;
}

// Tab Switching
function switchTab(tabId) {
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".tab-view").forEach(v => {
    v.classList.remove("active");
    v.classList.add("hidden");
  });

  const btn = document.getElementById(`tab-${tabId}`);
  const view = document.getElementById(`view-${tabId}`);
  if (btn) btn.classList.add("active");
  if (view) {
    view.classList.remove("hidden");
    view.classList.add("active");
  }

  if (tabId === "hdfs") loadHDFSFiles();
  if (tabId === "analytics") loadMetrics();
}

// HDFS Lake Explorer
async function loadHDFSFiles() {
  try {
    const res = await fetch("/api/hdfs/files");
    if (!res.ok) return;
    const data = await res.json();

    const tbody = document.getElementById("hdfs-tbody");
    const countEl = document.getElementById("val-hdfs-count");
    if (countEl) countEl.textContent = data.total_files || 0;
    if (!tbody) return;

    if (!data.files || data.files.length === 0) {
      tbody.innerHTML = `
        <tr class="empty-row">
          <td colspan="5">
            <div class="empty-state">
              <p>No partition files stored yet. Stream articles through Kafka to create date-partitioned HDFS files!</p>
            </div>
          </td>
        </tr>`;
      return;
    }

    tbody.innerHTML = data.files.map(f => `
      <tr>
        <td><code>${escapeHtml(f.path)}</code></td>
        <td><span class="badge badge-subtle">${f.storage_type}</span></td>
        <td>${f.length.toLocaleString()} B</td>
        <td>${f.replication}x</td>
        <td>
          ${!f.is_dir ? `<button class="btn btn-sm btn-outline-primary" onclick="inspectHDFSFile('${encodeURIComponent(f.path)}')">View Records</button>` : '<span class="text-dim">DIR</span>'}
        </td>
      </tr>
    `).join("");
  } catch (err) {
    console.error("Failed to load HDFS files:", err);
  }
}

async function inspectHDFSFile(encodedPath) {
  const path = decodeURIComponent(encodedPath);
  try {
    const res = await fetch(`/api/hdfs/content?path=${encodeURIComponent(path)}`);
    if (!res.ok) return;
    const data = await res.json();

    const box = document.getElementById("hdfs-preview-box");
    const title = document.getElementById("preview-filename");
    const jsonPre = document.getElementById("preview-json");

    if (box && title && jsonPre) {
      title.textContent = `HDFS File: ${path}`;
      jsonPre.textContent = JSON.stringify(data.records_sample || data.raw_snippet, null, 2);
      box.classList.remove("hidden");
    }
  } catch (err) {
    alert("Could not read HDFS file: " + err);
  }
}

function closeHDFSPreview() {
  document.getElementById("hdfs-preview-box")?.classList.add("hidden");
}

// Analytics View
async function loadMetrics() {
  try {
    const res = await fetch("/api/metrics");
    if (!res.ok) return;
    const data = await res.json();
    const mm = data.model_metrics;
    if (!mm) return;

    const pm = mm.primary_metrics || {};
    setElText("val-model-accuracy", `${(pm.accuracy * 100 || 99.38).toFixed(2)}%`);
    setElText("m-algo", mm.selected_model || "Logistic Regression");
    setElText("m-acc", `${(pm.accuracy * 100 || 0).toFixed(2)}%`);
    setElText("m-f1", `${(pm.f1 * 100 || 0).toFixed(2)}%`);
    setElText("m-prec", `${(pm.precision * 100 || 0).toFixed(2)}%`);
    setElText("m-rec", `${(pm.recall * 100 || 0).toFixed(2)}%`);
    setElText("m-auc", `${(pm.roc_auc * 100 || 0).toFixed(2)}%`);
    setElText("m-vocab", `${(mm.vocab_size || 10000).toLocaleString()} n-grams`);

    // Confusion matrix
    const cm = pm.confusion_matrix;
    if (cm && cm.length === 2) {
      setElText("cm-tn", `${cm[0][0]} (TN)`);
      setElText("cm-fp", `${cm[0][1]} (FP)`);
      setElText("cm-fn", `${cm[1][0]} (FN)`);
      setElText("cm-tp", `${cm[1][1]} (TP)`);
    }

    // Top words
    const tf = mm.top_features;
    if (tf) {
      const relContainer = document.getElementById("kw-reliable");
      const unrelContainer = document.getElementById("kw-unreliable");
      if (relContainer && tf.reliable_indicators) {
        relContainer.innerHTML = tf.reliable_indicators.map(w => `
          <span class="kw-chip rel">${escapeHtml(w.word)}</span>
        `).join("");
      }
      if (unrelContainer && tf.unreliable_indicators) {
        unrelContainer.innerHTML = tf.unreliable_indicators.map(w => `
          <span class="kw-chip unrel">${escapeHtml(w.word)}</span>
        `).join("");
      }
    }
  } catch (err) {
    console.warn("Metrics load error:", err);
  }
}

function setElText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

// Modal Article Inspection
function openArticleModal(index) {
  const art = streamEvents[index];
  if (!art) return;

  const modal = document.getElementById("article-modal");
  document.getElementById("modal-title").textContent = art.title;
  document.getElementById("modal-meta").textContent = `ID: ${art.article_id} | Source: ${art.source} | Processed: ${art.processed_at}`;
  
  const isRel = art.classification?.is_reliable;
  const label = art.classification?.label?.toUpperCase();
  const conf = Math.round((art.classification?.confidence || 0.5) * 100);

  document.getElementById("modal-label-box").innerHTML = `
    <span class="label-badge ${isRel ? 'reliable' : 'unreliable'}">${label} (${conf}% Confidence)</span>
  `;

  document.getElementById("modal-text").textContent = art.text_snippet || art.text || "No snippet available";

  const chips = document.getElementById("modal-tokens");
  const signals = art.classification?.top_signals || [];
  chips.innerHTML = signals.map(s => `
    <span class="token-chip ${s.indication}"><strong>${escapeHtml(s.word)}</strong> (${s.impact})</span>
  `).join("") || "<span class='text-dim'>None</span>";

  modal.classList.remove("hidden");
}

function closeArticleModal() {
  document.getElementById("article-modal")?.classList.add("hidden");
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
