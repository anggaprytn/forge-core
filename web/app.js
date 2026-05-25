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

const SEVERITY_ORDER = {
  critical: 0,
  warning: 1,
  info: 2,
};

const STATUS_ORDER = {
  active: 0,
  cleared: 1,
  historical: 2,
};

const TIMELINE_DEFAULT_VISIBLE = 5;

const uiState = {
  showAllCleared: false,
  showHistorical: false,
  showPastRecommendations: false,
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
  node.hidden = false;
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

function lower(value) {
  return text(value, "").trim().toLowerCase();
}

function isHealthy(readyzLike) {
  return Boolean(readyzLike) && readyzLike.status === "ready" && !readyzLike.active_failure;
}

function severityRank(value) {
  const normalized = lower(value);
  return Object.prototype.hasOwnProperty.call(SEVERITY_ORDER, normalized)
    ? SEVERITY_ORDER[normalized]
    : SEVERITY_ORDER.info;
}

function statusRank(value) {
  const normalized = lower(value);
  return Object.prototype.hasOwnProperty.call(STATUS_ORDER, normalized)
    ? STATUS_ORDER[normalized]
    : STATUS_ORDER.historical;
}

function normalizeStatus(value) {
  const normalized = lower(value);
  if (normalized === "active" || normalized === "cleared" || normalized === "historical") {
    return normalized;
  }
  return "historical";
}

function normalizeSeverity(value) {
  const normalized = lower(value);
  if (normalized === "critical" || normalized === "warning" || normalized === "info") {
    return normalized;
  }
  return "info";
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

function createBadge(label, tone) {
  const badge = document.createElement("span");
  badge.className = `item-badge ${tone}`;
  badge.textContent = label;
  return badge;
}

function appendNote(node, message, className) {
  if (!message) {
    return;
  }
  const note = document.createElement("p");
  note.className = className;
  note.textContent = message;
  node.appendChild(note);
}

function appendTimelineItem(list, options) {
  const item = document.createElement("li");
  item.className = `timeline-item ${options.tone}${options.muted ? " muted" : ""}`;

  const header = document.createElement("div");
  header.className = "timeline-item-header";

  const title = document.createElement("p");
  title.className = "timeline-item-title";
  title.textContent = options.title;
  header.appendChild(title);

  const badges = document.createElement("div");
  badges.className = "timeline-item-badges";
  badges.appendChild(createBadge(options.statusLabel, options.statusTone));
  if (options.severityLabel) {
    badges.appendChild(createBadge(options.severityLabel, options.severityTone));
  }
  header.appendChild(badges);

  item.appendChild(header);

  if (options.detail) {
    const body = document.createElement("p");
    body.className = "timeline-item-detail";
    body.textContent = options.detail;
    item.appendChild(body);
  }

  appendNote(item, options.note, "timeline-item-note");
  list.appendChild(item);
}

function appendRecommendationCard(container, recommendation, options) {
  const item = document.createElement("div");
  item.className = `recommendation-item ${options.tone}${options.muted ? " muted" : ""}`;

  const header = document.createElement("div");
  header.className = "recommendation-item-header";

  const title = document.createElement("p");
  title.className = "recommendation-title";
  title.textContent = recommendation.title;
  header.appendChild(title);

  const badges = document.createElement("div");
  badges.className = "recommendation-item-badges";
  badges.appendChild(createBadge(options.statusLabel, options.statusTone));
  if (options.severityLabel) {
    badges.appendChild(createBadge(options.severityLabel, options.severityTone));
  }
  header.appendChild(badges);

  item.appendChild(header);

  const body = document.createElement("p");
  body.className = "recommendation-detail";
  body.textContent = recommendation.description;
  item.appendChild(body);

  if (recommendation.commandHint) {
    appendNote(item, `Command: ${recommendation.commandHint}`, "recommendation-note");
  }

  if (options.note) {
    appendNote(item, options.note, "recommendation-note");
  }

  container.appendChild(item);
}

function appendRecommendationListItem(list, recommendation, options) {
  const item = document.createElement("li");
  appendRecommendationCard(item, recommendation, options);
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
  setBadge(
    "overview-badge",
    degraded ? PANEL_STATE.degraded : PANEL_STATE.ok,
    degraded ? "warn" : "ok",
  );
}

function renderReadiness(explain, readyz) {
  const grid = element("readiness-grid");
  if (!grid) {
    return;
  }

  const healthy = isHealthy(readyz) && !explain.active_failure;

  clearChildren(grid);
  appendField(grid, "Current readiness", text(explain.readiness_status));
  appendField(
    grid,
    "Primary state",
    healthy
      ? "No active readiness blockers."
      : text(explain.active_failure_reason || explain.operator_interpretation, "Unknown"),
  );
  appendField(
    grid,
    "Primary recommendation",
    healthy
      ? "No action required"
      : text(
          explain.summary && explain.summary.primary_recommendation
            ? explain.summary.primary_recommendation.title
            : explain.safe_next_action,
        ),
  );
  appendField(
    grid,
    "Historical note",
    explain.historical_failures
      ? `Historical failure recorded. Review timeline only if investigating a past incident. Last recorded event: ${formatUnix(explain.last_historical_failure_unix)}`
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

function compareRecommendations(a, b) {
  const statusDelta = statusRank(a.status) - statusRank(b.status);
  if (statusDelta !== 0) {
    return statusDelta;
  }
  const severityDelta = severityRank(a.severity) - severityRank(b.severity);
  if (severityDelta !== 0) {
    return severityDelta;
  }
  return text(a.title).localeCompare(text(b.title));
}

function recommendationKey(recommendation, fallbackSeed) {
  const actionId = text(recommendation.action_id, "").trim();
  if (actionId) {
    return actionId;
  }
  return `derived:${fallbackSeed}`;
}

function buildSyntheticRecommendation(entry) {
  return {
    action_id: `${normalizeStatus(entry.status)}:${text(entry.blocker_type, "blocker")}:${text(entry.suggested_action, "action")}`,
    severity: entry.active_failure ? "warning" : "info",
    title:
      normalizeStatus(entry.status) === "active"
        ? `Check ${text(entry.blocker_type, "blocker")}`
        : `Past ${text(entry.blocker_type, "blocker")} check`,
    description: text(entry.suggested_action, "No detail provided."),
    command_hint: "",
  };
}

function chooseBetterRecommendation(current, candidate) {
  const currentStatus = statusRank(current.status);
  const candidateStatus = statusRank(candidate.status);
  if (candidateStatus < currentStatus) {
    return candidate;
  }
  if (candidateStatus > currentStatus) {
    return current;
  }

  const currentSeverity = severityRank(current.severity);
  const candidateSeverity = severityRank(candidate.severity);
  if (candidateSeverity < currentSeverity) {
    return candidate;
  }
  if (candidateSeverity > currentSeverity) {
    return current;
  }

  return candidate.timestampUnix > current.timestampUnix ? candidate : current;
}

function dedupeRecommendations(explain, timeline) {
  const groupedStatuses = new Map();

  for (const entry of timeline.entries || []) {
    const status = normalizeStatus(entry.status);
    if (entry.recommendation && entry.recommendation.action_id) {
      const key = entry.recommendation.action_id;
      const seen = groupedStatuses.get(key) || new Set();
      seen.add(status);
      groupedStatuses.set(key, seen);
    }
  }

  const candidates = [];
  const explainRecommendations = [];

  if (explain.summary && explain.summary.primary_recommendation) {
    explainRecommendations.push(explain.summary.primary_recommendation);
  }
  for (const recommendation of explain.recommendations || []) {
    explainRecommendations.push(recommendation);
  }

  for (const recommendation of explainRecommendations) {
    const key = text(recommendation.action_id, "").trim();
    const seen = key ? groupedStatuses.get(key) : null;
    let status = "historical";
    if (seen && seen.has("active")) {
      status = "active";
    } else if (seen && seen.has("cleared")) {
      status = "cleared";
    } else if (explain.active_failure) {
      status = "active";
    } else if (explain.summary && explain.summary.cleared_count > 0) {
      status = "cleared";
    }

    candidates.push({
      key: recommendationKey(recommendation, `explain:${recommendation.title}:${recommendation.description}`),
      status,
      severity: normalizeSeverity(recommendation.severity),
      title: text(recommendation.title, "Untitled recommendation"),
      description: text(recommendation.description, "No detail provided."),
      commandHint: text(recommendation.command_hint, ""),
      timestampUnix: 0,
    });
  }

  for (const [index, entry] of (timeline.entries || []).entries()) {
    const recommendation = entry.recommendation || (entry.suggested_action ? buildSyntheticRecommendation(entry) : null);
    if (!recommendation) {
      continue;
    }

    candidates.push({
      key: recommendationKey(
        recommendation,
        `timeline:${index}:${entry.blocker_type}:${entry.suggested_action || entry.reason}`,
      ),
      status: normalizeStatus(entry.status),
      severity: normalizeSeverity(recommendation.severity || (entry.active_failure ? "warning" : "info")),
      title: text(recommendation.title, "Untitled recommendation"),
      description: text(recommendation.description, "No detail provided."),
      commandHint: text(recommendation.command_hint, ""),
      timestampUnix: entry.timestamp_unix || 0,
    });
  }

  const deduped = new Map();

  for (const candidate of candidates) {
    const existing = deduped.get(candidate.key);
    if (!existing) {
      deduped.set(candidate.key, {
        ...candidate,
        seenActive: candidate.status === "active",
        seenCleared: candidate.status === "cleared",
        seenHistorical: candidate.status === "historical",
      });
      continue;
    }

    existing.seenActive = existing.seenActive || candidate.status === "active";
    existing.seenCleared = existing.seenCleared || candidate.status === "cleared";
    existing.seenHistorical = existing.seenHistorical || candidate.status === "historical";

    const chosen = chooseBetterRecommendation(existing, candidate);
    deduped.set(candidate.key, {
      ...chosen,
      seenActive: existing.seenActive,
      seenCleared: existing.seenCleared,
      seenHistorical: existing.seenHistorical,
    });
  }

  const active = [];
  const cleared = [];
  const historical = [];

  for (const recommendation of deduped.values()) {
    if (recommendation.status === "active") {
      active.push(recommendation);
    } else if (recommendation.status === "cleared") {
      cleared.push(recommendation);
    } else {
      historical.push(recommendation);
    }
  }

  active.sort(compareRecommendations);
  cleared.sort(compareRecommendations);
  historical.sort(compareRecommendations);

  return { active, cleared, historical };
}

function recommendationNote(recommendation) {
  if (recommendation.status === "active") {
    if (recommendation.seenCleared) {
      return "Also seen in cleared history.";
    }
    if (recommendation.seenHistorical) {
      return "Also seen in historical entries.";
    }
    return "";
  }

  if (recommendation.status === "cleared") {
    if (recommendation.seenHistorical) {
      return "Previously observed, now cleared. Also seen in older history.";
    }
    return "Previously observed, now cleared.";
  }

  return "Historical recommendation only. Not an active readiness blocker.";
}

function renderTimelineEntry(list, entry) {
  const status = normalizeStatus(entry.status);
  const recommendation = entry.recommendation || null;
  const severity = normalizeSeverity(
    recommendation ? recommendation.severity : entry.active_failure ? "warning" : "info",
  );
  const title = `${text(entry.blocker_type)}: ${text(entry.reason)}`;

  if (status === "active") {
    appendTimelineItem(list, {
      title,
      detail: `Phase: ${text(entry.startup_phase)}.`,
      note: entry.suggested_action ? `Suggested check: ${entry.suggested_action}` : "",
      tone: "warn",
      muted: false,
      statusLabel: "Active",
      statusTone: "active",
      severityLabel: severity,
      severityTone: severity,
    });
    return;
  }

  if (status === "cleared") {
    appendTimelineItem(list, {
      title,
      detail: `Previously observed, now cleared. Recorded at ${formatUnix(entry.timestamp_unix)}.`,
      note: entry.suggested_action ? `Past suggested check: ${entry.suggested_action}` : "",
      tone: "cleared",
      muted: true,
      statusLabel: "Cleared",
      statusTone: "cleared",
      severityLabel: "",
      severityTone: "",
    });
    return;
  }

  const detail = lower(entry.blocker_type) === "convergence"
    ? "Historical convergence failure recorded."
    : `Recorded at ${formatUnix(entry.timestamp_unix)}.`;
  appendTimelineItem(list, {
    title,
    detail,
    note: lower(entry.blocker_type) === "convergence"
      ? "Not an active readiness blocker."
      : entry.suggested_action
        ? `Past suggested check: ${entry.suggested_action}`
        : "Not an active readiness blocker.",
    tone: "historical",
    muted: true,
    statusLabel: "Historical",
    statusTone: "historical",
    severityLabel: "",
    severityTone: "",
  });
}

function configureToggle(buttonId, visible, label, onClick) {
  const button = element(buttonId);
  if (!button) {
    return;
  }
  button.hidden = !visible;
  if (!visible) {
    button.textContent = "";
    button.onclick = null;
    return;
  }
  button.textContent = label;
  button.onclick = onClick;
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
    const status = normalizeStatus(entry.status);
    if (status === "active") {
      activeEntries.push(entry);
    } else if (status === "cleared") {
      clearedEntries.push(entry);
    } else {
      historicalEntries.push(entry);
    }
  }

  if (activeEntries.length === 0) {
    appendTimelineItem(active, {
      title: "No active blockers",
      detail: "Current readiness timeline shows no active blockers.",
      note: "",
      tone: "ok",
      muted: false,
      statusLabel: "Active",
      statusTone: "info",
      severityLabel: "",
      severityTone: "",
    });
  } else {
    for (const entry of activeEntries) {
      renderTimelineEntry(active, entry);
    }
  }

  const clearedVisible = uiState.showAllCleared ? clearedEntries : clearedEntries.slice(0, TIMELINE_DEFAULT_VISIBLE);
  if (clearedVisible.length === 0) {
    appendTimelineItem(cleared, {
      title: "No recently cleared blockers",
      detail: "No recent blocker recovery events reported.",
      note: "",
      tone: "historical",
      muted: true,
      statusLabel: "Cleared",
      statusTone: "cleared",
      severityLabel: "",
      severityTone: "",
    });
  } else {
    for (const entry of clearedVisible) {
      renderTimelineEntry(cleared, entry);
    }
  }

  const showHistoricalByDefault = activeEntries.length > 0;
  const historicalOpen = showHistoricalByDefault || uiState.showHistorical;
  if (historicalOpen) {
    for (const entry of historicalEntries) {
      renderTimelineEntry(historical, entry);
    }
  }
  if (!historicalOpen || historicalEntries.length === 0) {
    appendTimelineItem(historical, {
      title: historicalEntries.length === 0 ? "No historical entries" : "Historical entries hidden",
      detail:
        historicalEntries.length === 0
          ? "No older blocker history reported."
          : "Historical entries are collapsed while there are no active blockers.",
      note: "",
      tone: "historical",
      muted: true,
      statusLabel: "Historical",
      statusTone: "historical",
      severityLabel: "",
      severityTone: "",
    });
  }

  configureToggle(
    "timeline-cleared-toggle",
    clearedEntries.length > TIMELINE_DEFAULT_VISIBLE,
    uiState.showAllCleared ? "Show less" : "Show all",
    () => {
      uiState.showAllCleared = !uiState.showAllCleared;
      renderTimeline(timeline);
    },
  );

  configureToggle(
    "timeline-historical-toggle",
    historicalEntries.length > 0 && activeEntries.length === 0,
    historicalOpen ? "Hide history" : "Show history",
    () => {
      uiState.showHistorical = !uiState.showHistorical;
      renderTimeline(timeline);
    },
  );

  hideState("timeline-state");
  showContainer("timeline-groups");

  const stale = timeline.warning && timeline.warning.toLowerCase().includes("stale");
  const degraded = activeEntries.length > 0;
  const label = stale ? PANEL_STATE.stale : degraded ? PANEL_STATE.degraded : PANEL_STATE.ok;
  const tone = stale ? "stale" : degraded ? "warn" : "ok";
  setBadge("timeline-badge", label, tone);
}

function renderRecommendations(explain, timeline, readyz) {
  const primary = element("recommendation-primary");
  const list = element("recommendation-list");
  if (!primary || !list) {
    return;
  }

  clearChildren(primary);
  clearChildren(list);

  const deduped = dedupeRecommendations(explain, timeline);
  const healthy = isHealthy(readyz) && !deduped.active.length;

  if (healthy) {
    appendRecommendationCard(
      primary,
      {
        title: "No action required",
        description: "Review timeline only if investigating a past incident.",
        commandHint: "",
      },
      {
        tone: "ok",
        muted: false,
        statusLabel: "Active",
        statusTone: "info",
        severityLabel: "",
        severityTone: "",
        note: "",
      },
    );

    appendRecommendationListItem(
      list,
      {
        title: "Past notes available",
        description: "Cleared and historical guidance is muted here. Review the timeline only if you are investigating a past incident.",
        commandHint: "",
      },
      {
        tone: "historical",
        muted: true,
        statusLabel: "Historical",
        statusTone: "historical",
        severityLabel: "",
        severityTone: "",
        note: "",
      },
    );

    configureToggle("recommendation-notes-toggle", false, "", null);
  } else {
    const primaryRecommendations = deduped.active.length
      ? deduped.active
      : [
          {
            status: "active",
            severity: "warning",
            title: text(
              explain.summary && explain.summary.primary_recommendation
                ? explain.summary.primary_recommendation.title
                : explain.safe_next_action,
              "Review readiness state",
            ),
            description: text(
              explain.summary && explain.summary.primary_recommendation
                ? explain.summary.primary_recommendation.description
                : explain.safe_next_action,
              "No detail provided.",
            ),
            commandHint: text(
              explain.summary && explain.summary.primary_recommendation
                ? explain.summary.primary_recommendation.command_hint
                : "",
              "",
            ),
            seenCleared: false,
            seenHistorical: false,
          },
        ];

    for (const recommendation of primaryRecommendations) {
      appendRecommendationCard(primary, recommendation, {
        tone: "warn",
        muted: false,
        statusLabel: "Active",
        statusTone: "active",
        severityLabel: recommendation.severity,
        severityTone: recommendation.severity,
        note: recommendationNote(recommendation),
      });
    }

    const pastRecommendations = deduped.cleared.concat(deduped.historical);
    const showPast = uiState.showPastRecommendations;
    const visiblePast = showPast ? pastRecommendations : pastRecommendations.slice(0, 3);

    for (const recommendation of visiblePast) {
      const isCleared = recommendation.status === "cleared";
      appendRecommendationListItem(list, {
        title: isCleared ? `Cleared: ${recommendation.title}` : `Historical: ${recommendation.title}`,
        description: isCleared
          ? "Previously observed, now cleared."
          : "Historical readiness note recorded. Not an active readiness blocker.",
        commandHint: recommendation.commandHint,
      }, {
        tone: recommendation.status,
        muted: true,
        statusLabel: isCleared ? "Cleared" : "Historical",
        statusTone: recommendation.status,
        severityLabel: "",
        severityTone: "",
        note: isCleared
          ? `Past suggested check: ${recommendation.description}`
          : recommendationNote(recommendation),
      });
    }

    if (!visiblePast.length) {
      appendRecommendationListItem(
        list,
        {
          title: "No past notes",
          description: "No cleared or historical recommendation notes are recorded.",
          commandHint: "",
        },
        {
          tone: "historical",
          muted: true,
          statusLabel: "Historical",
          statusTone: "historical",
          severityLabel: "",
          severityTone: "",
          note: "",
        },
      );
    }

    configureToggle(
      "recommendation-notes-toggle",
      pastRecommendations.length > 3,
      showPast ? "Show fewer notes" : "Show all notes",
      () => {
        uiState.showPastRecommendations = !uiState.showPastRecommendations;
        renderRecommendations(explain, timeline, readyz);
      },
    );
  }

  hideState("recommendations-state");
  showContainer("recommendations-content");

  const degraded = explain.active_failure || deduped.active.length > 0;
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

  if (explain.status === "fulfilled" && readyz.status === "fulfilled") {
    renderReadiness(explain.value, readyz.value);
  } else if (explain.status === "fulfilled") {
    renderReadiness(explain.value, null);
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
    renderRecommendations(
      explain.value,
      timeline.value,
      readyz.status === "fulfilled" ? readyz.value : null,
    );
  } else {
    renderUnavailable(
      "recommendations-state",
      "recommendations-badge",
      "API unreachable for operator recommendations.",
    );
  }
}

void loadConsole();
