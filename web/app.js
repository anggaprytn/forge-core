const API_PATHS = {
  readyz: "/readyz",
  metrics: "/metrics",
  explain: "/readiness/explain",
  timeline: "/readiness/timeline",
};

const PANEL_STATE = {
  loading: "Loading",
  ok: "Live",
  degraded: "Degraded",
  stale: "Stale",
  unavailable: "Unavailable",
};

function element(id) {
  return document.getElementById(id);
}

async function fetchJson(path) {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
  });

  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }

  return response.json();
}

function setBadge(id, label, tone) {
  const node = element(id);
  if (!node) {
    return;
  }
  node.textContent = label;
  node.className = `status-pill ${tone}`;
}

function showState(id, message, tone) {
  const node = element(id);
  if (!node) {
    return;
  }
  node.textContent = message;
  node.className = `inline-state ${tone}`;
}

function hideState(id) {
  const node = element(id);
  if (node) {
    node.hidden = true;
  }
}

function showContainer(id) {
  const node = element(id);
  if (node) {
    node.hidden = false;
  }
}

function text(value, fallback = "Unknown") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function boolLabel(value) {
  return value ? "Yes" : "No";
}

function formatDuration(ms) {
  if (typeof ms !== "number" || !Number.isFinite(ms)) {
    return "Unknown";
  }
  if (ms < 1000) {
    return `${ms} ms`;
  }
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remSeconds = seconds % 60;
  if (minutes < 60) {
    return `${minutes}m ${remSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours}h ${remMinutes}m`;
}

function formatUnix(unixSeconds) {
  if (typeof unixSeconds !== "number" || unixSeconds <= 0) {
    return "None";
  }
  return new Date(unixSeconds * 1000).toLocaleString();
}

function clearChildren(node) {
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function appendField(container, label, value) {
  const term = document.createElement("dt");
  term.textContent = label;
  const description = document.createElement("dd");
  description.textContent = value;
  container.append(term, description);
}

function appendListItem(list, title, detail, tone) {
  const item = document.createElement("li");
  item.className = `timeline-item ${tone}`;

  const heading = document.createElement("p");
  heading.className = "timeline-item-title";
  heading.textContent = title;
  item.appendChild(heading);

  if (detail) {
    const body = document.createElement("p");
    body.className = "timeline-item-detail";
    body.textContent = detail;
    item.appendChild(body);
  }

  list.appendChild(item);
}

function appendRecommendation(list, recommendation) {
  const item = document.createElement("li");
  item.className = "recommendation-item";

  const title = document.createElement("p");
  title.className = "recommendation-title";
  title.textContent = text(recommendation.title, "Untitled recommendation");
  item.appendChild(title);

  const body = document.createElement("p");
  body.className = "recommendation-detail";
  const description = text(recommendation.description, "No detail provided.");
  const commandHint = recommendation.command_hint
    ? ` Command: ${recommendation.command_hint}`
    : "";
  body.textContent = `${description}${commandHint}`;
  item.appendChild(body);

  list.appendChild(item);
}

function renderOverview(readyz, metrics) {
  const grid = element("overview-grid");
  if (!grid) {
    return;
  }

  clearChildren(grid);
  appendField(grid, "Readiness status", text(readyz.status));
  appendField(grid, "Startup phase", text(readyz.startup_phase || metrics.startup_phase));
  appendField(
    grid,
    "Active failure",
    boolLabel(Boolean(readyz.active_failure || metrics.convergence_active_failure)),
  );
  appendField(grid, "Leader", boolLabel(Boolean(metrics.leader)));
  appendField(grid, "Follower mode", boolLabel(Boolean(metrics.follower_mode)));
  appendField(grid, "Replay in progress", boolLabel(Boolean(metrics.replay_in_progress)));
  appendField(grid, "Readiness cache age", formatDuration(metrics.readiness_cache_age_ms));
  appendField(
    grid,
    "Convergence loop duration",
    formatDuration(metrics.convergence_loop_duration_ms),
  );

  hideState("overview-state");
  showContainer("overview-grid");

  const degraded = readyz.status !== "ready" || readyz.active_failure;
  setBadge("overview-badge", degraded ? PANEL_STATE.degraded : PANEL_STATE.ok, degraded ? "warn" : "ok");
}

function renderReadiness(explain) {
  const grid = element("readiness-grid");
  if (!grid) {
    return;
  }

  clearChildren(grid);
  appendField(grid, "Current readiness", text(explain.readiness_status));
  appendField(
    grid,
    "Active blocker summary",
    text(explain.active_failure_reason || explain.operator_interpretation, "None"),
  );
  appendField(
    grid,
    "Primary recommendation",
    text(
      explain.summary && explain.summary.primary_recommendation
        ? explain.summary.primary_recommendation.title
        : explain.safe_next_action,
    ),
  );
  appendField(
    grid,
    "Historical note",
    explain.historical_failures
      ? `Historical failure recorded. Last historical failure: ${formatUnix(explain.last_historical_failure_unix)}`
      : "No historical failures reported.",
  );

  hideState("readiness-state");
  showContainer("readiness-grid");

  const stale = explain.warning && explain.warning.toLowerCase().includes("stale");
  const degraded = explain.readiness_status !== "ready" || explain.active_failure;
  const label = stale ? PANEL_STATE.stale : degraded ? PANEL_STATE.degraded : PANEL_STATE.ok;
  const tone = stale ? "stale" : degraded ? "warn" : "ok";
  setBadge("readiness-badge", label, tone);
}

function renderTimeline(timeline) {
  const active = element("timeline-active");
  const cleared = element("timeline-cleared");
  const historical = element("timeline-historical");
  if (!active || !cleared || !historical) {
    return;
  }

  clearChildren(active);
  clearChildren(cleared);
  clearChildren(historical);

  const activeEntries = [];
  const clearedEntries = [];
  const historicalEntries = [];

  for (const entry of timeline.entries || []) {
    if (entry.status === "active") {
      activeEntries.push(entry);
    } else if (entry.status === "cleared") {
      clearedEntries.push(entry);
    } else {
      historicalEntries.push(entry);
    }
  }

  for (const entry of activeEntries) {
    appendListItem(
      active,
      `${text(entry.blocker_type)}: ${text(entry.reason)}`,
      `Phase: ${text(entry.startup_phase)}. Suggested action: ${text(entry.suggested_action, "None")}`,
      "warn",
    );
  }
  for (const entry of clearedEntries) {
    appendListItem(
      cleared,
      `${text(entry.blocker_type)} cleared`,
      `${text(entry.reason)} at ${formatUnix(entry.timestamp_unix)}. Suggested action: ${text(entry.suggested_action, "None")}`,
      "ok",
    );
  }
  for (const entry of historicalEntries) {
    appendListItem(
      historical,
      `${text(entry.blocker_type)} history`,
      `${text(entry.reason)} at ${formatUnix(entry.timestamp_unix)}. Suggested action: ${text(entry.suggested_action, "None")}`,
      "muted",
    );
  }

  if (activeEntries.length === 0) {
    appendListItem(active, "No active blockers", "Current readiness timeline shows no active blockers.", "ok");
  }
  if (clearedEntries.length === 0) {
    appendListItem(cleared, "Nothing recently cleared", "No recent blocker recovery events reported.", "muted");
  }
  if (historicalEntries.length === 0) {
    appendListItem(historical, "No historical entries", "No older blocker history reported.", "muted");
  }

  hideState("timeline-state");
  showContainer("timeline-groups");

  const stale = timeline.warning && timeline.warning.toLowerCase().includes("stale");
  const degraded = activeEntries.length > 0;
  const label = stale ? PANEL_STATE.stale : degraded ? PANEL_STATE.degraded : PANEL_STATE.ok;
  const tone = stale ? "stale" : degraded ? "warn" : "ok";
  setBadge("timeline-badge", label, tone);
}

function renderRecommendations(explain, timeline) {
  const primary = element("recommendation-primary");
  const list = element("recommendation-list");
  if (!primary || !list) {
    return;
  }

  clearChildren(primary);
  clearChildren(list);

  const primaryRecommendation =
    (explain.summary && explain.summary.primary_recommendation) || explain.recommendations[0];

  if (primaryRecommendation) {
    const title = document.createElement("p");
    title.className = "recommendation-title";
    title.textContent = text(primaryRecommendation.title);
    primary.appendChild(title);

    const detail = document.createElement("p");
    detail.className = "recommendation-detail";
    const commandHint = primaryRecommendation.command_hint
      ? ` Command: ${primaryRecommendation.command_hint}`
      : "";
    detail.textContent = `${text(primaryRecommendation.description, "No detail provided.")}${commandHint}`;
    primary.appendChild(detail);
  } else {
    const fallback = document.createElement("p");
    fallback.className = "recommendation-detail";
    fallback.textContent = text(explain.safe_next_action, "No action required.");
    primary.appendChild(fallback);
  }

  for (const recommendation of explain.recommendations || []) {
    appendRecommendation(list, recommendation);
  }

  for (const entry of timeline.entries || []) {
    if (entry.suggested_action) {
      appendRecommendation(list, {
        title: `${text(entry.blocker_type)} suggested action`,
        description: entry.suggested_action,
        command_hint: entry.recommendation ? entry.recommendation.command_hint : "",
      });
    }
  }

  if (!list.children.length) {
    appendRecommendation(list, {
      title: "No additional recommendations",
      description: text(explain.safe_next_action, "No action required."),
      command_hint: "",
    });
  }

  hideState("recommendations-state");
  showContainer("recommendations-content");

  const degraded = explain.active_failure || (timeline.entries || []).some((entry) => entry.status === "active");
  setBadge(
    "recommendations-badge",
    degraded ? PANEL_STATE.degraded : PANEL_STATE.ok,
    degraded ? "warn" : "ok",
  );
}

function renderMetrics(metrics) {
  const grid = element("metrics-grid");
  if (!grid) {
    return;
  }

  clearChildren(grid);
  appendField(grid, "Queue depth", text(metrics.queue_depth, "0"));
  appendField(grid, "Pending intents", text(metrics.pending_intents, "0"));
  appendField(grid, "Replay queue depth", text(metrics.replay_queue_depth, "0"));
  appendField(grid, "Readyz latency", formatDuration(metrics.readyz_latency_ms));
  appendField(grid, "Docker probe latency", formatDuration(metrics.docker_probe_latency_ms));
  appendField(grid, "Caddy probe latency", formatDuration(metrics.caddy_probe_latency_ms));
  appendField(grid, "Convergence failures total", text(metrics.convergence_failures_total, "0"));
  appendField(grid, "Last historical failure", formatUnix(metrics.convergence_last_failure_historical_unix));

  hideState("metrics-state");
  showContainer("metrics-grid");

  const readinessReason = text(metrics.readiness_reason, "").toLowerCase();
  const activeFailureReason = text(metrics.convergence_active_failure_reason, "").toLowerCase();
  const stale = readinessReason.includes("stale") || activeFailureReason.includes("stale");
  const degraded = Boolean(metrics.convergence_active_failure);
  const label = stale ? PANEL_STATE.stale : degraded ? PANEL_STATE.degraded : PANEL_STATE.ok;
  const tone = stale ? "stale" : degraded ? "warn" : "ok";
  setBadge("metrics-badge", label, tone);
}

function renderUnavailable(panel, badge, message) {
  showState(panel, message, "warn");
  setBadge(badge, PANEL_STATE.unavailable, "warn");
}

async function loadConsole() {
  const [readyz, metrics, explain, timeline] = await Promise.allSettled([
    fetchJson(API_PATHS.readyz),
    fetchJson(API_PATHS.metrics),
    fetchJson(API_PATHS.explain),
    fetchJson(API_PATHS.timeline),
  ]);

  if (readyz.status === "fulfilled" && metrics.status === "fulfilled") {
    renderOverview(readyz.value, metrics.value);
  } else {
    renderUnavailable("overview-state", "overview-badge", "API unreachable for readiness overview.");
  }

  if (explain.status === "fulfilled") {
    renderReadiness(explain.value);
  } else {
    renderUnavailable("readiness-state", "readiness-badge", "Readiness degraded details unavailable.");
  }

  if (timeline.status === "fulfilled") {
    renderTimeline(timeline.value);
  } else {
    renderUnavailable("timeline-state", "timeline-badge", "Timeline unavailable.");
  }

  if (metrics.status === "fulfilled") {
    renderMetrics(metrics.value);
  } else {
    renderUnavailable("metrics-state", "metrics-badge", "Metrics unavailable.");
  }

  if (explain.status === "fulfilled" && timeline.status === "fulfilled") {
    renderRecommendations(explain.value, timeline.value);
  } else {
    renderUnavailable(
      "recommendations-state",
      "recommendations-badge",
      "API unreachable for operator recommendations.",
    );
  }
}

void loadConsole();
