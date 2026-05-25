const API_PATHS = {
  readyz: "/readyz",
  metrics: "/metrics",
  explain: "/readiness/explain",
  timeline: "/readiness/timeline",
  projects: "/api/projects",
  projectEnvironments(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments`;
  },
};

const uiState = {
  query: "",
  selectedProjectId: "",
};

const dataState = {
  readyz: null,
  metrics: null,
  explain: null,
  timeline: null,
  projects: [],
  environmentsByProject: new Map(),
};

function element(id) {
  return document.getElementById(id);
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

function clearChildren(node) {
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function formatUnix(unixSeconds) {
  if (typeof unixSeconds !== "number" || unixSeconds <= 0) {
    return "Unknown";
  }
  return new Date(unixSeconds * 1000).toLocaleString();
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

function fetchJson(path) {
  return fetch(path, {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
  }).then((response) => {
    if (!response.ok) {
      const error = new Error(`request failed: ${response.status}`);
      error.status = response.status;
      throw error;
    }
    return response.json();
  });
}

async function fetchApiData(path) {
  const payload = await fetchJson(path);
  return payload && Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}

function toneFromReadyz(readyz, explain) {
  if (readyz && readyz.status === "ready" && !readyz.active_failure && !(explain && explain.active_failure)) {
    return "ok";
  }
  if (!readyz) {
    return "stale";
  }
  return "warn";
}

function toneFromHealthState(value) {
  const health = lower(value);
  if (health === "healthy" || health === "ready") {
    return "ok";
  }
  if (health === "degraded" || health === "failed" || health === "unhealthy") {
    return "warn";
  }
  if (health === "unavailable" || health === "missing") {
    return "stale";
  }
  return "";
}

function setSummary(id, value, note, tone = "") {
  const valueNode = element(id);
  const noteNode = element(`${id}-note`);
  if (valueNode) {
    valueNode.textContent = value;
    valueNode.className = `summary-value${tone ? ` ${tone}` : ""}`;
  }
  if (noteNode) {
    noteNode.textContent = note;
  }
}

function setChip(id, label, tone = "") {
  const node = element(id);
  if (!node) {
    return;
  }
  node.textContent = label;
  node.className = `status-chip${tone ? ` ${tone}` : ""}`;
}

function showState(id, message, tone = "") {
  const node = element(id);
  if (!node) {
    return;
  }
  node.hidden = false;
  node.textContent = message;
  node.className = `inline-state${tone ? ` ${tone}` : ""}`;
}

function hideState(id) {
  const node = element(id);
  if (node) {
    node.hidden = true;
  }
}

function show(id) {
  const node = element(id);
  if (node) {
    node.hidden = false;
  }
}

function hide(id) {
  const node = element(id);
  if (node) {
    node.hidden = true;
  }
}

function projectList() {
  return Array.isArray(dataState.projects) ? dataState.projects : [];
}

function filteredProjects() {
  const query = lower(uiState.query);
  if (!query) {
    return projectList();
  }

  return projectList().filter((project) => {
    const haystacks = [
      project.project_id,
      project.base_domain,
      project.default_branch,
      project.repo_url,
    ];
    return haystacks.some((value) => lower(value).includes(query));
  });
}

function selectedProject() {
  return projectList().find((project) => project.project_id === uiState.selectedProjectId) || null;
}

function activeTimelineEntries() {
  const entries = dataState.timeline && Array.isArray(dataState.timeline.entries)
    ? dataState.timeline.entries
    : [];
  return entries.filter((entry) => lower(entry.status) === "active");
}

function inventoryStats(projects) {
  let environments = 0;
  let degraded = 0;

  for (const project of projects) {
    for (const environment of project.environments || []) {
      environments += 1;
      const health = lower(environment.readiness_summary && environment.readiness_summary.health_state);
      if (health && health !== "healthy") {
        degraded += 1;
      }
    }
  }

  return { environments, degraded };
}

function primaryReadyzLabel() {
  if (!dataState.readyz) {
    return "Unavailable";
  }
  if (toneFromReadyz(dataState.readyz, dataState.explain) === "ok") {
    return "Ready";
  }
  if (dataState.explain && dataState.explain.active_failure_reason) {
    return text(dataState.explain.active_failure_reason);
  }
  return text(dataState.readyz.status, "Unavailable");
}

function renderSummary() {
  const projects = projectList();
  const blockers = activeTimelineEntries();
  const inventory = inventoryStats(projects);
  const readyTone = toneFromReadyz(dataState.readyz, dataState.explain);
  const readyNote = dataState.metrics
    ? `Leader ${dataState.metrics.leader ? "active" : "inactive"} • cache age ${formatDuration(dataState.metrics.readiness_cache_age_ms)}`
    : "Metrics unavailable.";

  setSummary("summary-readyz", primaryReadyzLabel(), readyNote, readyTone);
  setSummary(
    "summary-blockers",
    String(blockers.length),
    blockers.length ? "Current issues need review." : "No active readiness blockers.",
    blockers.length ? "warn" : "ok",
  );
  setSummary(
    "summary-projects",
    String(projects.length),
    projects.length ? "Registered projects." : "No projects registered yet.",
    projects.length ? "" : "stale",
  );
  setSummary(
    "summary-environments",
    inventory.environments ? `${inventory.degraded}/${inventory.environments}` : "0/0",
    inventory.environments ? "Degraded versus total lanes." : "No environments registered yet.",
    inventory.degraded ? "warn" : inventory.environments ? "ok" : "stale",
  );
}

function renderProjects() {
  const projects = filteredProjects();
  const container = element("project-list");
  if (!container) {
    return;
  }

  clearChildren(container);
  setChip("project-count-chip", `${projects.length} project${projects.length === 1 ? "" : "s"}`);

  if (!projectList().length) {
    showState("project-list-state", "No projects registered yet.");
    hide("project-list");
    return;
  }

  if (!projects.length) {
    showState("project-list-state", "No project matches that search.");
    hide("project-list");
    return;
  }

  hideState("project-list-state");
  show("project-list");

  for (const project of projects) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `project-item${project.project_id === uiState.selectedProjectId ? " is-active" : ""}`;
    button.addEventListener("click", () => {
      uiState.selectedProjectId = project.project_id;
      renderProjects();
      void renderSelectedProject();
    });

    const titleRow = document.createElement("div");
    titleRow.className = "project-item-row";

    const title = document.createElement("p");
    title.className = "project-item-title";
    title.textContent = project.project_id;

    const tag = document.createElement("span");
    tag.className = "status-chip";
    tag.textContent = `${(project.environments || []).length} env`;

    titleRow.append(title, tag);

    const meta = document.createElement("p");
    meta.className = "project-item-meta";
    meta.textContent = text(project.base_domain);

    const note = document.createElement("p");
    note.className = "project-item-note";
    note.textContent = `Branch ${text(project.default_branch)} • ${text(project.repo_url)}`;

    button.append(titleRow, meta, note);
    container.appendChild(button);
  }
}

async function ensureProjectEnvironments(projectId) {
  if (!projectId) {
    return [];
  }
  if (dataState.environmentsByProject.has(projectId)) {
    return dataState.environmentsByProject.get(projectId) || [];
  }

  try {
    const payload = await fetchApiData(API_PATHS.projectEnvironments(projectId));
    const environments = Array.isArray(payload.environments) ? payload.environments : [];
    dataState.environmentsByProject.set(projectId, environments);
    return environments;
  } catch (_error) {
    throw new Error("Environment inventory unavailable.");
  }
}

function appendMeta(container, label, value) {
  const wrapper = document.createElement("div");
  const term = document.createElement("dt");
  term.textContent = label;
  const description = document.createElement("dd");
  description.textContent = value;
  wrapper.append(term, description);
  container.appendChild(wrapper);
}

function environmentCard(environment) {
  const card = document.createElement("article");
  const tone = toneFromHealthState(environment.readiness_summary && environment.readiness_summary.health_state);
  card.className = `environment-card${tone ? ` ${tone}` : ""}`;

  const header = document.createElement("div");
  header.className = "environment-card-head";

  const title = document.createElement("div");
  const name = document.createElement("p");
  name.className = "environment-name";
  name.textContent = text(environment.environment);
  const route = document.createElement("p");
  route.className = "environment-route";
  route.textContent = text(environment.route, "Route not set");
  title.append(name, route);

  const chip = document.createElement("span");
  chip.className = `status-chip${tone ? ` ${tone}` : ""}`;
  chip.textContent = text(
    environment.readiness_summary && environment.readiness_summary.health_state,
    environment.last_deployment_status || "Unknown",
  );

  header.append(title, chip);
  card.appendChild(header);

  const facts = document.createElement("dl");
  facts.className = "environment-facts";
  appendMeta(facts, "Current", environment.current_generation === null || environment.current_generation === undefined ? "None" : `Gen ${environment.current_generation}`);
  appendMeta(facts, "Previous", environment.previous_generation === null || environment.previous_generation === undefined ? "None" : `Gen ${environment.previous_generation}`);
  appendMeta(facts, "Deploy", text(environment.last_deployment_status));
  appendMeta(
    facts,
    "Last success",
    formatUnix(
      environment.readiness_summary && environment.readiness_summary.last_successful_convergence_unix,
    ),
  );
  card.appendChild(facts);

  const reasons = environment.readiness_summary && Array.isArray(environment.readiness_summary.reasons)
    ? environment.readiness_summary.reasons.filter(Boolean)
    : [];
  const note = document.createElement("p");
  note.className = "environment-note";
  note.textContent = reasons.length ? reasons[0] : "No additional readiness notes.";
  card.appendChild(note);

  return card;
}

async function renderSelectedProject() {
  const project = selectedProject();
  if (!project) {
    hide("project-detail");
    showState("project-detail-state", "Project no longer exists or has no registered environments.");
    setChip("project-health-chip", "No project", "stale");
    setChip("environment-count-chip", "0 env", "stale");
    return;
  }

  element("project-name").textContent = project.project_id;
  element("project-domain").textContent = text(project.base_domain);
  element("project-repo").textContent = text(project.repo_url);
  element("project-base-domain").textContent = text(project.base_domain);
  setChip("project-branch", `Branch ${text(project.default_branch)}`);
  setChip("project-updated", `Updated ${formatUnix(project.updated_at_unix)}`);

  hide("project-detail");
  showState("project-detail-state", "Loading environment inventory...");

  try {
    const environments = await ensureProjectEnvironments(project.project_id);
    if (!environments.length) {
      showState("project-detail-state", "Project no longer exists or has no registered environments.");
      setChip("project-health-chip", "No environments", "stale");
      setChip("environment-count-chip", "0 env", "stale");
      return;
    }

    const grid = element("environment-grid");
    clearChildren(grid);
    let degraded = 0;

    for (const environment of environments) {
      const tone = toneFromHealthState(environment.readiness_summary && environment.readiness_summary.health_state);
      if (tone === "warn" || tone === "stale") {
        degraded += 1;
      }
      grid.appendChild(environmentCard(environment));
    }

    setChip(
      "project-health-chip",
      degraded ? `${degraded} need focus` : "Quiet",
      degraded ? "warn" : "ok",
    );
    setChip(
      "environment-count-chip",
      `${environments.length} env`,
      degraded ? "warn" : "",
    );
    hideState("project-detail-state");
    show("project-detail");
  } catch (error) {
    showState("project-detail-state", error.message || "Environment inventory unavailable.", "warn");
    setChip("project-health-chip", "Unavailable", "stale");
    setChip("environment-count-chip", "0 env", "stale");
  }
}

function signalCard(title, body, tone, meta = "") {
  const card = document.createElement("article");
  card.className = `signal-card${tone ? ` ${tone}` : ""}`;

  const header = document.createElement("div");
  header.className = "signal-card-head";

  const heading = document.createElement("p");
  heading.className = "signal-title";
  heading.textContent = title;

  const chip = document.createElement("span");
  chip.className = `status-chip${tone ? ` ${tone}` : ""}`;
  chip.textContent = tone === "warn" ? "Active" : tone === "ok" ? "Clear" : "Info";

  header.append(heading, chip);
  card.appendChild(header);

  const copy = document.createElement("p");
  copy.className = "signal-copy";
  copy.textContent = body;
  card.appendChild(copy);

  if (meta) {
    const note = document.createElement("p");
    note.className = "signal-meta";
    note.textContent = meta;
    card.appendChild(note);
  }

  return card;
}

function renderSignals() {
  const list = element("signals-list");
  if (!list) {
    return;
  }

  clearChildren(list);

  const blockers = activeTimelineEntries();
  const explain = dataState.explain;

  if (blockers.length) {
    blockers.slice(0, 4).forEach((entry) => {
      list.appendChild(
        signalCard(
          text(entry.blocker_type, "Blocker"),
          text(entry.reason, "No reason provided."),
          "warn",
          entry.suggested_action ? `Suggested check: ${entry.suggested_action}` : "",
        ),
      );
    });
    setChip("signals-chip", `${blockers.length} active`, "warn");
    hideState("signals-state");
    show("signals-list");
    return;
  }

  if (explain) {
    list.appendChild(
      signalCard(
        "Readiness",
        explain.active_failure
          ? text(explain.active_failure_reason, "Readiness degraded details unavailable.")
          : "No active readiness blockers.",
        explain.active_failure ? "warn" : "ok",
        explain.warning ? text(explain.warning) : "",
      ),
    );

    if (dataState.metrics) {
      list.appendChild(
        signalCard(
          "Control plane",
          `Startup ${text(dataState.metrics.startup_phase)} • replay ${dataState.metrics.replay_in_progress ? "running" : "quiet"}.`,
          "",
          `Metrics cache age ${formatDuration(dataState.metrics.readiness_cache_age_ms)}.`,
        ),
      );
    }

    setChip("signals-chip", explain.active_failure ? "Needs review" : "Quiet", explain.active_failure ? "warn" : "ok");
    hideState("signals-state");
    show("signals-list");
    return;
  }

  showState("signals-state", "Timeline unavailable.", "warn");
  setChip("signals-chip", "Unavailable", "stale");
  hide("signals-list");
}

async function loadConsole() {
  const refreshButton = element("refresh-button");
  if (refreshButton) {
    refreshButton.disabled = true;
    refreshButton.textContent = "Refreshing...";
  }

  const [readyzResult, metricsResult, explainResult, timelineResult, projectsResult] = await Promise.allSettled([
    fetchApiData(API_PATHS.readyz),
    fetchApiData(API_PATHS.metrics),
    fetchApiData(API_PATHS.explain),
    fetchApiData(API_PATHS.timeline),
    fetchApiData(API_PATHS.projects),
  ]);

  dataState.readyz = readyzResult.status === "fulfilled" ? readyzResult.value : null;
  dataState.metrics = metricsResult.status === "fulfilled" ? metricsResult.value : null;
  dataState.explain = explainResult.status === "fulfilled" ? explainResult.value : null;
  dataState.timeline = timelineResult.status === "fulfilled" ? timelineResult.value : null;

  if (projectsResult.status === "fulfilled") {
    const payload = projectsResult.value;
    dataState.projects = Array.isArray(payload.projects) ? payload.projects : [];
  } else {
    dataState.projects = [];
    showState("project-list-state", "Project inventory unavailable.", "warn");
  }

  if (!dataState.readyz) {
    setSummary("summary-readyz", "Unavailable", "API unreachable for readiness overview.", "stale");
  }
  if (!dataState.metrics) {
    setSummary("summary-readyz", primaryReadyzLabel(), "Metrics unavailable.", toneFromReadyz(dataState.readyz, dataState.explain));
  }
  if (!dataState.explain && dataState.readyz) {
    setSummary("summary-readyz", primaryReadyzLabel(), "Readiness degraded details unavailable.", toneFromReadyz(dataState.readyz, dataState.explain));
  }
  if (!dataState.timeline) {
    showState("signals-state", "Timeline unavailable.", "warn");
  }

  if (!uiState.selectedProjectId || !projectList().some((project) => project.project_id === uiState.selectedProjectId)) {
    uiState.selectedProjectId = projectList()[0] ? projectList()[0].project_id : "";
  }

  renderSummary();
  renderProjects();
  renderSignals();
  await renderSelectedProject();

  if (refreshButton) {
    refreshButton.disabled = false;
    refreshButton.textContent = "Refresh";
  }
}

function bindControls() {
  const search = element("project-search");
  if (search) {
    search.addEventListener("input", (event) => {
      uiState.query = event.target.value || "";
      renderProjects();
    });
  }

  const refreshButton = element("refresh-button");
  if (refreshButton) {
    refreshButton.addEventListener("click", () => {
      void loadConsole();
    });
  }
}

bindControls();
void loadConsole();
