const API_PATHS = {
  readyz: "/readyz",
  metrics: "/metrics",
  explain: "/readiness/explain",
  timeline: "/readiness/timeline",
  projects: "/api/projects",
};

const PANEL_STATE = {
  loading: "Loading",
  ok: "Live",
  degraded: "Degraded",
  stale: "Stale",
  unavailable: "Unavailable",
};

const ROUTES = {
  overview: {
    group: "Operate",
    title: "Overview",
    description: "Current control-plane posture, blockers, and operator next actions.",
  },
  monitoring: {
    group: "Operate",
    title: "Monitoring",
    description: "Readiness state, probe latency, queue pressure, and live health interpretation.",
  },
  logs: {
    group: "Operate",
    title: "Logs",
    description: "Timeline-first view for historical readiness events and recovery patterns.",
  },
  deployments: {
    group: "Build & Ship",
    title: "Deployments",
    description: "Environment lane state, rollback posture, and selected release context.",
  },
  runtime: {
    group: "Build & Ship",
    title: "Runtime",
    description: "Execution lane health, active services, and environment runtime posture.",
  },
  infrastructure: {
    group: "Platform",
    title: "Infrastructure",
    description: "Projects, routing identity, and environment inventory in a master-detail workspace.",
  },
  "ai-systems": {
    group: "AI",
    title: "AI Systems",
    description: "Reserved shell surface for retrieval, orchestration, and model operations telemetry.",
  },
  settings: {
    group: "Admin",
    title: "Settings",
    description: "Access posture, policy boundaries, and reserved control-plane administration surfaces.",
  },
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
  route: "overview",
  showAllCleared: false,
  showHistorical: false,
  showPastRecommendations: false,
  projectQuery: "",
  selectedProjectId: "",
  sidebarOpen: false,
};

const inventoryState = {
  projects: [],
  environmentsByProject: new Map(),
};

const consoleState = {
  readyz: null,
  metrics: null,
  explain: null,
  timeline: null,
};

let controlsBound = false;

function element(id) {
  return document.getElementById(id);
}

async function fetchJson(path) {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
  });

  if (!response.ok) {
    const error = new Error(`request failed: ${response.status}`);
    error.status = response.status;
    throw error;
  }

  return response.json();
}

async function fetchApiData(path) {
  const payload = await fetchJson(path);
  return payload && Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}

function text(value, fallback = "Unknown") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function lower(value) {
  return text(value, "").trim().toLowerCase();
}

function boolLabel(value) {
  return value ? "Yes" : "No";
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

function showContainer(id) {
  const node = element(id);
  if (node) {
    node.hidden = false;
  }
}

function hideState(id) {
  const node = element(id);
  if (node) {
    node.hidden = true;
  }
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

function setBadge(id, label, tone) {
  const node = element(id);
  if (!node) {
    return;
  }
  node.textContent = label;
  node.className = `status-pill ${tone}`;
}

function setKpi(id, value, tone) {
  const node = element(id);
  if (!node) {
    return;
  }
  node.textContent = value;
  node.className = `kpi-value${tone ? ` ${tone}` : ""}`;
}

function appendField(container, label, value) {
  const term = document.createElement("dt");
  term.textContent = label;
  const description = document.createElement("dd");
  description.textContent = value;
  container.append(term, description);
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

function createBadge(label, tone) {
  const badge = document.createElement("span");
  badge.className = `item-badge ${tone}`;
  badge.textContent = label;
  return badge;
}

function appendSummaryField(container, label, value) {
  const row = document.createElement("div");
  row.className = "summary-field";
  const term = document.createElement("p");
  term.className = "summary-field-label";
  term.textContent = label;
  const description = document.createElement("p");
  description.className = "summary-field-value";
  description.textContent = value;
  row.append(term, description);
  container.appendChild(row);
}

function appendSignalCard(container, title, value, note, tone = "") {
  const card = document.createElement("div");
  card.className = "signal-card";
  const heading = document.createElement("p");
  heading.className = "summary-field-label";
  heading.textContent = title;
  const strong = document.createElement("p");
  strong.className = `summary-field-value${tone ? ` ${tone}` : ""}`;
  strong.textContent = value;
  card.append(heading, strong);
  appendNote(card, note, "page-note");
  container.appendChild(card);
}

function inventoryEnvironmentStats(projects) {
  let total = 0;
  let degraded = 0;

  for (const project of projects || []) {
    for (const environment of project.environments || []) {
      total += 1;
      const health = lower(environment.readiness_summary && environment.readiness_summary.health_state);
      if (health && health !== "healthy") {
        degraded += 1;
      }
    }
  }

  return { total, degraded };
}

function activeTimelineEntries() {
  return consoleState.timeline && consoleState.timeline.entries
    ? consoleState.timeline.entries.filter((entry) => normalizeStatus(entry.status) === "active")
    : [];
}

function statusTone(status) {
  const normalized = lower(status);
  if (normalized === "healthy" || normalized === "promoted" || normalized === "live" || normalized === "ready") {
    return "ok";
  }
  if (normalized === "missing" || normalized === "unavailable") {
    return "stale";
  }
  return "warn";
}

function generationLabel(value) {
  return value === null || value === undefined ? "None" : `Gen ${value}`;
}

function projectDomainSource(project) {
  const mode = lower(project.domain_mode);
  const baseDomain = lower(project.base_domain);
  const prefix = `${lower(project.project_id)}.`;
  if (mode === "explicit") {
    return "explicit";
  }
  if (mode === "generated") {
    return baseDomain.startsWith(prefix) ? "generated" : "generated fallback";
  }
  return "unknown";
}

function readinessTone(readiness) {
  const state = lower(readiness && readiness.health_state);
  if (state === "healthy") {
    return "ok";
  }
  if (state === "degraded") {
    return "warn";
  }
  if (state === "unavailable") {
    return "stale";
  }
  return "neutral";
}

function readinessStateLabel(readiness) {
  return readiness ? text(readiness.health_state) : "Unknown";
}

function environmentCardTone(environment) {
  const readiness = lower(environment.readiness_summary && environment.readiness_summary.health_state);
  if (readiness === "degraded") {
    return "readiness-degraded";
  }
  if (readiness === "unavailable") {
    return "readiness-unavailable";
  }
  if (readiness === "healthy") {
    return "readiness-healthy";
  }
  return "readiness-unknown";
}

function selectedProject() {
  return inventoryState.projects.find((project) => project.project_id === uiState.selectedProjectId) || null;
}

function selectedEnvironments() {
  if (!uiState.selectedProjectId) {
    return [];
  }
  if (inventoryState.environmentsByProject.has(uiState.selectedProjectId)) {
    return inventoryState.environmentsByProject.get(uiState.selectedProjectId) || [];
  }
  const project = selectedProject();
  return project ? project.environments || [] : [];
}

function updateRouteMeta() {
  const meta = ROUTES[uiState.route] || ROUTES.overview;
  const eyebrow = element("current-view-eyebrow");
  const title = element("current-view-title");
  const description = element("current-view-description");
  if (eyebrow) {
    eyebrow.textContent = meta.group;
  }
  if (title) {
    title.textContent = meta.title;
  }
  if (description) {
    description.textContent = meta.description;
  }
}

function updateRouteVisibility() {
  for (const view of document.querySelectorAll("[data-route-view]")) {
    view.hidden = view.dataset.routeView !== uiState.route;
  }

  for (const button of document.querySelectorAll("[data-route]")) {
    button.classList.toggle("is-active", button.dataset.route === uiState.route);
  }

  updateRouteMeta();
}

function syncRouteFromHash() {
  const route = window.location.hash ? window.location.hash.slice(1) : "overview";
  uiState.route = Object.prototype.hasOwnProperty.call(ROUTES, route) ? route : "overview";
  updateRouteVisibility();
  setSidebarOpen(false);
}

function setSidebarOpen(nextOpen) {
  uiState.sidebarOpen = nextOpen;
  const sidebar = element("app-sidebar");
  const scrim = element("sidebar-scrim");
  if (sidebar) {
    sidebar.classList.toggle("is-open", nextOpen);
  }
  if (scrim) {
    scrim.hidden = !nextOpen;
  }
}

function navigate(route) {
  if (!Object.prototype.hasOwnProperty.call(ROUTES, route)) {
    return;
  }
  window.location.hash = route;
}

function focusInventorySearch() {
  window.requestAnimationFrame(() => {
    const search = element("inventory-search");
    if (!search) {
      return;
    }
    search.focus();
    search.select();
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

function bindGlobalControls() {
  if (controlsBound) {
    return;
  }
  controlsBound = true;

  const refresh = element("action-refresh");
  if (refresh) {
    refresh.addEventListener("click", () => {
      refresh.disabled = true;
      refresh.textContent = "Refreshing...";
      void loadConsole(true).finally(() => {
        refresh.disabled = false;
        refresh.textContent = "Refresh state";
      });
    });
  }

  const searchTrigger = element("utility-search-trigger");
  if (searchTrigger) {
    searchTrigger.addEventListener("click", () => {
      navigate("infrastructure");
      focusInventorySearch();
    });
  }

  const sidebarToggle = element("sidebar-toggle");
  if (sidebarToggle) {
    sidebarToggle.addEventListener("click", () => {
      setSidebarOpen(!uiState.sidebarOpen);
    });
  }

  const sidebarScrim = element("sidebar-scrim");
  if (sidebarScrim) {
    sidebarScrim.addEventListener("click", () => setSidebarOpen(false));
  }

  for (const button of document.querySelectorAll("[data-route]")) {
    button.addEventListener("click", () => navigate(button.dataset.route));
  }

  for (const button of document.querySelectorAll("[data-group-toggle]")) {
    button.addEventListener("click", () => {
      const expanded = button.getAttribute("aria-expanded") === "true";
      button.setAttribute("aria-expanded", expanded ? "false" : "true");
    });
  }

  window.addEventListener("hashchange", syncRouteFromHash);
  syncRouteFromHash();

  document.addEventListener("keydown", (event) => {
    const target = event.target;
    const tagName = target && target.tagName ? target.tagName.toLowerCase() : "";
    const editing = tagName === "input" || tagName === "textarea" || (target && target.isContentEditable);
    if (editing) {
      return;
    }

    if (event.key === "/") {
      navigate("infrastructure");
      event.preventDefault();
      focusInventorySearch();
    }
  });
}

function renderOperationalSummary(readyz, explain, timeline, projects) {
  const projectCount = (projects || []).length;
  const { total, degraded } = inventoryEnvironmentStats(projects || []);
  const activeBlockers = (timeline && timeline.entries
    ? timeline.entries.filter((entry) => normalizeStatus(entry.status) === "active")
    : []).length;
  const controlPlaneState = readyz && readyz.status === "ready" && !readyz.active_failure
    ? "Ready"
    : explain && explain.active_failure
      ? "Blocked"
      : readyz
        ? text(readyz.status)
        : "Unavailable";

  setKpi(
    "kpi-control-plane",
    controlPlaneState,
    controlPlaneState === "Ready" ? "ok" : controlPlaneState === "Unavailable" ? "stale" : "warn",
  );
  setKpi("kpi-blockers", String(activeBlockers), activeBlockers > 0 ? "warn" : "ok");
  setKpi("kpi-projects", String(projectCount), projectCount > 0 ? "" : "stale");
  setKpi(
    "kpi-environments",
    total ? `${degraded}/${total}` : "0/0",
    degraded > 0 ? "warn" : total > 0 ? "ok" : "stale",
  );

  setBadge(
    "global-context-posture",
    `Posture: ${controlPlaneState}`,
    controlPlaneState === "Ready" ? "ok" : controlPlaneState === "Unavailable" ? "stale" : "warn",
  );
  setBadge(
    "global-context-blockers",
    activeBlockers ? `${activeBlockers} active blockers` : "No active blockers",
    activeBlockers ? "warn" : "ok",
  );
  setBadge(
    "global-context-project",
    uiState.selectedProjectId ? `Project ${uiState.selectedProjectId}` : "Project context unset",
    uiState.selectedProjectId ? "info" : "stale",
  );
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
  appendField(grid, "Convergence loop duration", formatDuration(metrics.convergence_loop_duration_ms));

  hideState("overview-state");
  showContainer("overview-grid");

  const degraded = readyz.status !== "ready" || readyz.active_failure;
  setBadge("overview-badge", degraded ? PANEL_STATE.degraded : PANEL_STATE.ok, degraded ? "warn" : "ok");
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
      ? `Historical failure recorded. Last recorded event: ${formatUnix(explain.last_historical_failure_unix)}`
      : "No historical failures reported.",
  );

  hideState("readiness-state");
  showContainer("readiness-grid");

  const stale = explain.warning && lower(explain.warning).includes("stale");
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
      renderDerivedViews();
    },
  );

  configureToggle(
    "timeline-historical-toggle",
    historicalEntries.length > 0 && activeEntries.length === 0,
    historicalOpen ? "Hide history" : "Show history",
    () => {
      uiState.showHistorical = !uiState.showHistorical;
      renderTimeline(timeline);
      renderDerivedViews();
    },
  );

  hideState("timeline-state");
  showContainer("timeline-groups");

  const stale = timeline.warning && lower(timeline.warning).includes("stale");
  const degraded = activeEntries.length > 0;
  const label = stale ? PANEL_STATE.stale : degraded ? PANEL_STATE.degraded : PANEL_STATE.ok;
  const tone = stale ? "stale" : degraded ? "warn" : "ok";
  setBadge("timeline-badge", label, tone);
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
        renderDerivedViews();
      },
    );
  }

  hideState("recommendations-state");
  showContainer("recommendations-content");

  const degraded = explain.active_failure || deduped.active.length > 0;
  setBadge("recommendations-badge", degraded ? PANEL_STATE.degraded : PANEL_STATE.ok, degraded ? "warn" : "ok");
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

function projectInventoryFilter(project) {
  if (!uiState.projectQuery) {
    return true;
  }
  return lower(project.project_id).includes(lower(uiState.projectQuery));
}

function bindInventoryControls() {
  const search = element("inventory-search");
  if (!search || search.dataset.bound === "true") {
    return;
  }
  search.dataset.bound = "true";
  search.addEventListener("input", () => {
    uiState.projectQuery = search.value.trim();
    renderProjectInventory();
    renderDerivedViews();
  });
}

function renderProjectInventory() {
  const list = element("inventory-list");
  if (!list) {
    return;
  }

  bindInventoryControls();
  clearChildren(list);

  const filtered = inventoryState.projects.filter(projectInventoryFilter);
  if (!filtered.length) {
    showState(
      "inventory-state",
      inventoryState.projects.length ? "No projects match the current filter." : "No projects registered yet.",
      "stale",
    );
    list.hidden = true;
    showState(
      "project-detail-state",
      inventoryState.projects.length
        ? "Clear or change the filter to inspect an environment lane."
        : "Select a project to inspect its environments.",
      inventoryState.projects.length ? "stale" : "ok",
    );
    const detail = element("project-detail");
    if (detail) {
      detail.hidden = true;
    }
    setBadge("inventory-badge", PANEL_STATE.stale, "stale");
    setBadge("project-detail-badge", PANEL_STATE.stale, "stale");
    return;
  }

  hideState("inventory-state");
  showContainer("inventory-list");

  if (!uiState.selectedProjectId || !filtered.some((project) => project.project_id === uiState.selectedProjectId)) {
    uiState.selectedProjectId = filtered[0].project_id;
  }

  for (const project of filtered) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `inventory-project${project.project_id === uiState.selectedProjectId ? " is-selected" : ""}`;
    button.addEventListener("click", () => {
      uiState.selectedProjectId = project.project_id;
      renderProjectInventory();
      renderDerivedViews();
      void loadProjectEnvironments(project.project_id);
    });

    const header = document.createElement("div");
    header.className = "inventory-project-header";

    const titleWrap = document.createElement("div");
    const label = document.createElement("p");
    label.className = "inventory-section-label";
    label.textContent = "Project";
    titleWrap.appendChild(label);

    const title = document.createElement("p");
    title.className = "inventory-project-title";
    title.textContent = project.project_id;
    titleWrap.appendChild(title);

    header.appendChild(titleWrap);

    const status = document.createElement("span");
    const firstStatus = (project.environments || []).find((environment) => environment.last_deployment_status);
    status.className = `status-pill ${statusTone(firstStatus && firstStatus.last_deployment_status)}`;
    status.textContent = firstStatus ? text(firstStatus.last_deployment_status) : "Inventory";
    header.appendChild(status);
    button.appendChild(header);

    const summary = document.createElement("div");
    summary.className = "inventory-summary-grid";
    appendSummaryField(summary, "Project ID", text(project.project_id));
    appendSummaryField(summary, "Base domain", text(project.base_domain, "Unknown"));
    appendSummaryField(summary, "Domain source", projectDomainSource(project));
    appendSummaryField(summary, "Default branch", text(project.default_branch));
    appendSummaryField(
      summary,
      "Environments",
      project.environments.length
        ? project.environments.map((environment) => environment.environment).join(", ")
        : "None discovered",
    );
    button.appendChild(summary);

    const badges = document.createElement("div");
    badges.className = "inventory-project-badges";
    for (const environment of project.environments || []) {
      badges.appendChild(createBadge(environment.environment, readinessTone(environment.readiness_summary)));
      if (environment.current_generation !== null && environment.current_generation !== undefined) {
        badges.appendChild(createBadge(`Current ${generationLabel(environment.current_generation)}`, "info"));
      }
    }
    button.appendChild(badges);

    const note = document.createElement("p");
    note.className = "inventory-project-note";
    note.textContent = project.environments.length
      ? `${project.environments.length} environment views available. Project ID remains the stable Forge identity; base domain is the routing identity.`
      : "No environments discovered yet.";
    button.appendChild(note);

    list.appendChild(button);
  }

  setBadge("inventory-badge", PANEL_STATE.ok, "ok");
}

function renderProjectEnvironmentDetails(projectId, environments) {
  const container = element("project-detail");
  if (!container) {
    return;
  }

  clearChildren(container);
  const project = inventoryState.projects.find((entry) => entry.project_id === projectId);
  if (project) {
    const header = document.createElement("div");
    header.className = "project-detail-header";
    const label = document.createElement("p");
    label.className = "inventory-section-label";
    label.textContent = "Project";
    header.appendChild(label);
    const title = document.createElement("p");
    title.className = "inventory-project-title";
    title.textContent = project.project_id;
    header.appendChild(title);
    const note = document.createElement("p");
    note.className = "inventory-project-note";
    note.textContent = `Read-only environment inventory for ${text(project.base_domain, "this project")}. Domain source: ${projectDomainSource(project)}.`;
    header.appendChild(note);
    container.appendChild(header);
  }

  if (!environments.length) {
    showState("project-detail-state", "No environments registered for this project.", "ok");
    container.hidden = true;
    setBadge("project-detail-badge", PANEL_STATE.stale, "stale");
    renderDerivedViews();
    return;
  }

  hideState("project-detail-state");
  showContainer("project-detail");

  for (const environment of environments) {
    const card = document.createElement("section");
    card.className = `environment-card ${environmentCardTone(environment)}`;

    const header = document.createElement("div");
    header.className = "environment-card-header";
    const titleWrap = document.createElement("div");
    const label = document.createElement("p");
    label.className = "inventory-section-label";
    label.textContent = "Environment";
    titleWrap.appendChild(label);
    const title = document.createElement("p");
    title.className = "environment-card-title";
    title.textContent = environment.environment;
    titleWrap.appendChild(title);
    header.appendChild(titleWrap);

    const badges = document.createElement("div");
    badges.className = "environment-card-badges";
    const statusBadge = createBadge(`Status: ${text(environment.last_deployment_status, "Unknown")}`, statusTone(environment.last_deployment_status));
    const readinessBadge = createBadge(`Readiness: ${readinessStateLabel(environment.readiness_summary)}`, readinessTone(environment.readiness_summary));
    badges.append(statusBadge, readinessBadge);
    header.appendChild(badges);
    card.appendChild(header);

    const semanticNote = document.createElement("p");
    semanticNote.className = "environment-card-note";
    semanticNote.textContent = "Status reflects deployment lifecycle. Readiness reflects the live environment lane and is separate from control-plane readiness.";
    card.appendChild(semanticNote);

    const grid = document.createElement("dl");
    grid.className = "environment-detail-grid";
    appendField(grid, "Environment", text(environment.environment));
    appendField(grid, "Status", text(environment.last_deployment_status, "Unknown"));
    appendField(grid, "Readiness", readinessStateLabel(environment.readiness_summary));
    appendField(grid, "Current generation", generationLabel(environment.current_generation));
    appendField(grid, "Previous generation", generationLabel(environment.previous_generation));
    appendField(
      grid,
      "Rollback eligibility",
      environment.rollback_eligibility
        ? `${environment.rollback_eligibility.eligible ? "Eligible" : "Not eligible"}: ${environment.rollback_eligibility.reason}`
        : "Unknown",
    );
    appendField(grid, "Active route", text(environment.route, "None"));
    appendField(grid, "Last deployment", formatUnix(environment.last_deployment_timestamp));
    appendField(
      grid,
      "Runtime policy",
      environment.runtime_policy ? `${text(environment.runtime_policy.restart_policy)} restart` : "Unknown",
    );
    appendField(
      grid,
      "Restore lineage",
      environment.restore_lineage
        ? `Backup ${environment.restore_lineage.backup_id} from ${generationLabel(environment.restore_lineage.source_generation)}`
        : "None",
    );
    appendField(
      grid,
      "Active services",
      (environment.active_services || []).length
        ? environment.active_services.map((service) => service.service_id).join(", ")
        : "Service metadata not recorded for this generation.",
    );
    appendField(grid, "Control-plane readiness", "See monitoring surfaces for operator and convergence state.");
    card.appendChild(grid);

    if (environment.active_services && environment.active_services.length) {
      const note = document.createElement("p");
      note.className = "environment-card-note";
      note.textContent = environment.active_services
        .map((service) => `${service.service_id} (${service.role}${service.route ? ` via ${service.route}` : ""})`)
        .join(" • ");
      card.appendChild(note);
    }

    if (environment.readiness_summary && environment.readiness_summary.reasons && environment.readiness_summary.reasons.length) {
      const note = document.createElement("p");
      note.className = "environment-card-note";
      note.textContent = `Readiness detail: ${environment.readiness_summary.reasons.join(" | ")}`;
      card.appendChild(note);
    }

    container.appendChild(card);
  }

  setBadge("project-detail-badge", PANEL_STATE.ok, "ok");
  renderDerivedViews();
}

function renderEmptySummary(containerId, title, note) {
  const container = element(containerId);
  if (!container) {
    return;
  }
  clearChildren(container);
  appendSignalCard(container, title, "Unavailable", note, "stale");
}

function renderOverviewInventorySummary() {
  const container = element("overview-projects-summary");
  if (!container) {
    return;
  }
  clearChildren(container);

  if (!inventoryState.projects.length) {
    appendSignalCard(container, "Inventory", "No projects", "Project inventory is empty or unavailable.", "stale");
    return;
  }

  const { total, degraded } = inventoryEnvironmentStats(inventoryState.projects);
  appendSignalCard(container, "Projects", String(inventoryState.projects.length), "Registered stable identities.");
  appendSignalCard(container, "Environment lanes", total ? String(total) : "0", "Total discovered execution lanes.");
  appendSignalCard(
    container,
    "Attention required",
    degraded ? String(degraded) : "0",
    degraded ? "Environment lanes with degraded or unavailable readiness." : "No degraded environment lanes detected.",
    degraded ? "warn" : "ok",
  );
}

function renderInfrastructureSummary() {
  const container = element("infrastructure-summary");
  if (!container) {
    return;
  }
  clearChildren(container);

  if (!inventoryState.projects.length) {
    appendSignalCard(container, "Infrastructure", "Unavailable", "Project inventory is not currently loaded.", "stale");
    return;
  }

  const { total, degraded } = inventoryEnvironmentStats(inventoryState.projects);
  appendSummaryField(container, "Projects", String(inventoryState.projects.length));
  appendSummaryField(container, "Environment lanes", String(total));
  appendSummaryField(container, "Degraded lanes", String(degraded));
  appendSummaryField(
    container,
    "Routing model",
    inventoryState.projects.some((project) => projectDomainSource(project) === "explicit")
      ? "Mixed explicit and generated domains"
      : "Generated routing identity",
  );
}

function renderSelectedProjectSummary() {
  const container = element("selected-project-summary");
  if (!container) {
    return;
  }
  clearChildren(container);

  const project = selectedProject();
  if (!project) {
    appendSignalCard(container, "Selected project", "None", "Pick a project from inventory to inspect routing and lane state.", "stale");
    return;
  }

  const environments = selectedEnvironments();
  appendSummaryField(container, "Project ID", text(project.project_id));
  appendSummaryField(container, "Base domain", text(project.base_domain, "Unknown"));
  appendSummaryField(container, "Default branch", text(project.default_branch));
  appendSummaryField(
    container,
    "Environment count",
    environments.length ? String(environments.length) : String((project.environments || []).length),
  );
}

function renderDeploymentSummary() {
  const container = element("deployment-summary");
  if (!container) {
    return;
  }
  clearChildren(container);

  const project = selectedProject();
  const environments = selectedEnvironments();
  if (!project) {
    appendSignalCard(container, "Release lanes", "No project selected", "Select a project in Infrastructure to inspect deployment posture.", "stale");
    return;
  }

  if (!environments.length) {
    appendSignalCard(container, "Release lanes", "No environments", "Selected project has no loaded environment lanes.", "stale");
    return;
  }

  for (const environment of environments) {
    appendSignalCard(
      container,
      `${environment.environment} lane`,
      text(environment.last_deployment_status, "Unknown"),
      `${generationLabel(environment.current_generation)} current • ${generationLabel(environment.previous_generation)} previous`,
      statusTone(environment.last_deployment_status),
    );
  }
}

function renderDeploymentProjectFocus() {
  const container = element("deployment-project-focus");
  if (!container) {
    return;
  }
  clearChildren(container);

  const project = selectedProject();
  const environments = selectedEnvironments();
  if (!project || !environments.length) {
    appendSignalCard(container, "Rollback posture", "Unavailable", "Load a project environment lane to inspect rollback eligibility.", "stale");
    return;
  }

  for (const environment of environments) {
    const rollback = environment.rollback_eligibility
      ? environment.rollback_eligibility.eligible
        ? `Eligible: ${environment.rollback_eligibility.reason}`
        : `Blocked: ${environment.rollback_eligibility.reason}`
      : "Unknown";
    appendSummaryField(container, `${environment.environment} rollback`, rollback);
  }
}

function renderRuntimeSummary() {
  const container = element("runtime-summary");
  if (!container) {
    return;
  }
  clearChildren(container);

  if (!consoleState.metrics || !consoleState.readyz) {
    appendSignalCard(container, "Runtime", "Unavailable", "Queue and runtime metrics have not loaded.", "stale");
    return;
  }

  appendSummaryField(container, "Readiness", text(consoleState.readyz.status));
  appendSummaryField(container, "Queue depth", text(consoleState.metrics.queue_depth, "0"));
  appendSummaryField(container, "Pending intents", text(consoleState.metrics.pending_intents, "0"));
  appendSummaryField(container, "Replay queue", text(consoleState.metrics.replay_queue_depth, "0"));
}

function renderRuntimeServices() {
  const container = element("runtime-services");
  if (!container) {
    return;
  }
  clearChildren(container);

  const environments = selectedEnvironments();
  if (!environments.length) {
    appendSignalCard(container, "Services", "No project context", "Select a project to inspect active service routing.", "stale");
    return;
  }

  let serviceCount = 0;
  for (const environment of environments) {
    const services = environment.active_services || [];
    if (!services.length) {
      appendSummaryField(container, `${environment.environment} services`, "No active service metadata");
      continue;
    }

    serviceCount += services.length;
    appendSummaryField(
      container,
      `${environment.environment} services`,
      services.map((service) => `${service.service_id}${service.route ? ` via ${service.route}` : ""}`).join(", "),
    );
  }

  if (serviceCount === 0) {
    appendSignalCard(container, "Services", "Unavailable", "Active service metadata is not recorded for the selected project.", "stale");
  }
}

function renderMonitoringMirror() {
  const container = element("monitoring-timeline-mirror");
  if (!container) {
    return;
  }
  clearChildren(container);

  if (!consoleState.timeline) {
    appendSignalCard(container, "Events", "Unavailable", "Timeline data has not loaded.", "stale");
    return;
  }

  const active = activeTimelineEntries();
  const clearedCount = (consoleState.timeline.entries || []).filter((entry) => normalizeStatus(entry.status) === "cleared").length;
  const historicalCount = (consoleState.timeline.entries || []).filter((entry) => normalizeStatus(entry.status) === "historical").length;

  appendSignalCard(container, "Active blockers", String(active.length), active.length ? "Action required now." : "No active readiness blockers.", active.length ? "warn" : "ok");
  appendSignalCard(container, "Recovered recently", String(clearedCount), "Recently cleared entries from the event stream.");
  appendSignalCard(container, "Historical pattern", String(historicalCount), "Older incidents preserved for investigation.");
}

function renderLogsEmbed() {
  const container = element("logs-timeline-embed");
  if (!container) {
    return;
  }
  clearChildren(container);

  if (!consoleState.timeline) {
    appendSignalCard(container, "Timeline", "Unavailable", "Timeline data has not loaded.", "stale");
    return;
  }

  const active = activeTimelineEntries();
  if (!active.length) {
    appendSignalCard(container, "Current stream", "Quiet", "No active readiness blockers in the event stream.", "ok");
  } else {
    for (const entry of active.slice(0, 4)) {
      appendSignalCard(
        container,
        text(entry.blocker_type, "Blocker"),
        text(entry.reason, "Unknown"),
        entry.suggested_action ? `Suggested check: ${entry.suggested_action}` : `Recorded ${formatUnix(entry.timestamp_unix)}`,
        "warn",
      );
    }
  }
}

function renderDerivedViews() {
  renderOverviewInventorySummary();
  renderInfrastructureSummary();
  renderSelectedProjectSummary();
  renderDeploymentSummary();
  renderDeploymentProjectFocus();
  renderRuntimeSummary();
  renderRuntimeServices();
  renderMonitoringMirror();
  renderLogsEmbed();
  renderOperationalSummary(
    consoleState.readyz,
    consoleState.explain,
    consoleState.timeline,
    inventoryState.projects,
  );
}

async function loadProjectEnvironments(projectId) {
  if (!projectId) {
    return;
  }
  if (inventoryState.environmentsByProject.has(projectId)) {
    renderProjectEnvironmentDetails(projectId, inventoryState.environmentsByProject.get(projectId) || []);
    return;
  }

  showState("project-detail-state", "Loading environment inventory...", "ok");
  const detail = element("project-detail");
  if (detail) {
    detail.hidden = true;
  }

  try {
    const payload = await fetchApiData(`${API_PATHS.projects}/${encodeURIComponent(projectId)}/environments`);
    const environments = payload && payload.environments ? payload.environments : [];
    inventoryState.environmentsByProject.set(projectId, environments);
    renderProjectEnvironmentDetails(projectId, environments);
  } catch (err) {
    if (err && err.status === 404) {
      inventoryState.environmentsByProject.delete(projectId);
      showState("project-detail-state", "Project no longer exists or has no registered environments.", "ok");
      if (detail) {
        detail.hidden = true;
      }
      setBadge("project-detail-badge", PANEL_STATE.stale, "stale");
      renderDerivedViews();
      return;
    }
    renderUnavailable("project-detail-state", "project-detail-badge", "Environment inventory unavailable.");
    renderDerivedViews();
  }
}

async function loadInventory(forceReload = false) {
  bindInventoryControls();
  if (forceReload) {
    inventoryState.environmentsByProject.clear();
  }

  const payload = await fetchApiData(API_PATHS.projects);
  inventoryState.projects = payload && payload.projects ? payload.projects : [];
  renderProjectInventory();
  renderDerivedViews();
  if (uiState.selectedProjectId) {
    await loadProjectEnvironments(uiState.selectedProjectId);
  }
}

function renderUnavailable(panel, badge, message) {
  showState(panel, message, "warn");
  setBadge(badge, PANEL_STATE.unavailable, "warn");
}

async function loadConsole(forceReload = false) {
  bindGlobalControls();

  const [readyz, metrics, explain, timeline, inventory] = await Promise.allSettled([
    fetchJson(API_PATHS.readyz),
    fetchJson(API_PATHS.metrics),
    fetchJson(API_PATHS.explain),
    fetchJson(API_PATHS.timeline),
    loadInventory(forceReload),
  ]);

  consoleState.readyz = readyz.status === "fulfilled" ? readyz.value : null;
  consoleState.metrics = metrics.status === "fulfilled" ? metrics.value : null;
  consoleState.explain = explain.status === "fulfilled" ? explain.value : null;
  consoleState.timeline = timeline.status === "fulfilled" ? timeline.value : null;

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

  if (inventory.status === "rejected") {
    renderUnavailable("inventory-state", "inventory-badge", "Project inventory unavailable.");
    renderUnavailable("project-detail-state", "project-detail-badge", "Environment inventory unavailable.");
    renderEmptySummary("overview-projects-summary", "Inventory", "Project inventory is unavailable.");
    renderEmptySummary("infrastructure-summary", "Infrastructure", "Project inventory is unavailable.");
    renderEmptySummary("selected-project-summary", "Project context", "Project inventory is unavailable.");
  }

  renderDerivedViews();
}

void loadConsole();
