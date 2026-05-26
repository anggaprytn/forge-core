const API_PATHS = {
  readyz: "/readyz",
  metrics: "/metrics",
  explain: "/readiness/explain",
  timeline: "/readiness/timeline",
  projects: "/api/projects",
  projectEnvironments(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments`;
  },
  projectEnvInventory(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/env`;
  },
  projectEnvPreview(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/env/preview`;
  },
  projectEnvApply(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/env/apply`;
  },
  projectEnvAudit(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/env/audit`;
  },
};

const uiState = {
  query: "",
  selectedProjectId: "",
  envQuery: "",
};

const dataState = {
  readyz: null,
  metrics: null,
  explain: null,
  timeline: null,
  projects: [],
  environmentsByProject: new Map(),
  envInventoryByProject: new Map(),
  envAuditByProject: new Map(),
  lastEnvPreview: null,
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

function fetchJson(path, options = {}) {
  const init = {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
    ...options,
  };
  init.headers = {
    Accept: "application/json",
    ...(options.headers || {}),
  };
  return fetch(path, init).then((response) => {
    if (!response.ok) {
      const error = new Error(`request failed: ${response.status}`);
      error.status = response.status;
      response
        .json()
        .then((payload) => {
          if (payload && payload.message) {
            error.message = payload.message;
          }
        })
        .catch(() => {});
      throw error;
    }
    return response.json();
  });
}

async function fetchApiData(path, options) {
  const payload = await fetchJson(path, options);
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

function filteredEnvVariables(inventory) {
  const variables = inventory && Array.isArray(inventory.variables) ? inventory.variables : [];
  const query = lower(uiState.envQuery);
  if (!query) {
    return variables;
  }
  return variables.filter((entry) => lower(entry.key).includes(query));
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

async function ensureProjectEnvInventory(projectId) {
  if (!projectId) {
    return null;
  }
  if (dataState.envInventoryByProject.has(projectId)) {
    return dataState.envInventoryByProject.get(projectId) || null;
  }

  try {
    const payload = await fetchApiData(API_PATHS.projectEnvInventory(projectId));
    dataState.envInventoryByProject.set(projectId, payload);
    return payload;
  } catch (_error) {
    throw new Error("Env inventory unavailable.");
  }
}

async function ensureProjectEnvAudit(projectId) {
  if (!projectId) {
    return null;
  }
  if (dataState.envAuditByProject.has(projectId)) {
    return dataState.envAuditByProject.get(projectId) || null;
  }

  try {
    const payload = await fetchApiData(API_PATHS.projectEnvAudit(projectId));
    dataState.envAuditByProject.set(projectId, payload);
    return payload;
  } catch (_error) {
    throw new Error("Env audit unavailable.");
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

function envColumnNames(inventory) {
  const environments = inventory && Array.isArray(inventory.environments) ? inventory.environments : [];
  return environments.length ? environments : ["development", "staging", "production"];
}

function setEnvSourceMeta(inventory) {
  const note = inventory && inventory.partial_metadata_note
    ? inventory.partial_metadata_note
    : inventory && inventory.partial_metadata_notice
      ? inventory.partial_metadata_notice
      : "Masked values only.";
  element("env-source-label").textContent = inventory ? text(inventory.source_label) : "Unknown source";
  element("env-source-note").textContent = note;
  show("env-inventory-meta");
}

function renderEnvInventory(inventory) {
  const table = element("env-inventory");
  const body = element("env-table-body");
  const chip = element("env-total-chip");
  const filter = filteredEnvVariables(inventory);
  const total = inventory && typeof inventory.total_variables === "number"
    ? inventory.total_variables
    : 0;
  const environments = envColumnNames(inventory);

  clearChildren(body);
  hide("env-inventory");
  hide("env-inventory-meta");

  if (!inventory || !total) {
    chip.textContent = "0 vars";
    showState("env-inventory-state", "No environment variables recorded for this project yet.");
    return;
  }

  setEnvSourceMeta(inventory);
  chip.textContent = uiState.envQuery && filter.length !== total
    ? `${filter.length}/${total} vars`
    : `${total} vars`;

  if (!filter.length) {
    showState("env-inventory-state", "No environment variable matches that search.");
    return;
  }

  hideState("env-inventory-state");
  show("env-inventory");

  for (const row of filter) {
    const tr = document.createElement("tr");
    const keyCell = document.createElement("th");
    keyCell.scope = "row";
    keyCell.className = "env-key";
    keyCell.textContent = row.key;
    tr.appendChild(keyCell);

    for (const environment of environments) {
      const td = document.createElement("td");
      const wrapper = document.createElement("div");
      wrapper.className = "env-cell";

      const cell = row.environments && row.environments[environment]
        ? row.environments[environment]
        : { exists: false, value: "missing" };

      const presence = document.createElement("div");
      presence.className = `env-presence ${cell.exists ? "exists" : "missing"}`;
      const dot = document.createElement("span");
      dot.className = "env-presence-dot";
      const label = document.createElement("span");
      label.textContent = cell.exists ? "present" : "missing";
      presence.append(dot, label);

      const value = document.createElement("p");
      value.className = `env-value${cell.exists ? "" : " missing"}`;
      value.textContent = text(cell.value, "missing");

      wrapper.append(presence, value);
      td.appendChild(wrapper);
      tr.appendChild(td);
    }

    body.appendChild(tr);
  }

  if (table) {
    const headCells = table.querySelectorAll("thead th");
    if (headCells.length >= 4) {
      headCells[1].textContent = "Development";
      headCells[2].textContent = "Staging";
      headCells[3].textContent = "Production";
      environments.forEach((environment, index) => {
        if (headCells[index + 1]) {
          headCells[index + 1].textContent = text(environment);
        }
      });
    }
  }
}

async function renderSelectedProject() {
  const project = selectedProject();
  if (!project) {
    hide("project-detail");
    showState("project-detail-state", "Project no longer exists or has no registered environments.");
    setChip("project-health-chip", "No project", "stale");
    setChip("environment-count-chip", "0 env", "stale");
    showState("env-inventory-state", "Select a project to inspect masked environment keys.");
    hide("env-inventory");
    hide("env-inventory-meta");
    setChip("env-total-chip", "0 vars", "stale");
    resetEnvPreview("Select a project to preview masked environment changes.", true);
    renderEnvAuditHistory(null);
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
  hide("env-inventory");
  hide("env-inventory-meta");
  showState("env-inventory-state", "Loading masked environment inventory...");
  resetEnvPreview("Preview only. No changes will be saved.", false);
  showState("env-audit-state", "Loading masked audit history...");
  hide("env-audit-history");

  try {
    const [environments, inventory, audit] = await Promise.all([
      ensureProjectEnvironments(project.project_id),
      ensureProjectEnvInventory(project.project_id),
      ensureProjectEnvAudit(project.project_id),
    ]);
    if (!environments.length) {
      showState("project-detail-state", "Project no longer exists or has no registered environments.");
      setChip("project-health-chip", "No environments", "stale");
      setChip("environment-count-chip", "0 env", "stale");
      renderEnvInventory(inventory);
      renderEnvAuditHistory(audit);
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
    renderEnvInventory(inventory);
    renderEnvAuditHistory(audit);
  } catch (error) {
    showState("project-detail-state", error.message || "Environment inventory unavailable.", "warn");
    showState("env-inventory-state", error.message || "Env inventory unavailable.", "warn");
    setChip("project-health-chip", "Unavailable", "stale");
    setChip("environment-count-chip", "0 env", "stale");
    setChip("env-total-chip", "0 vars", "stale");
    showState("env-preview-state", "Preview unavailable until project inventory loads.", "warn");
    showState("env-audit-state", error.message || "Audit history unavailable.", "warn");
  }
}

function previewTextArea(environment) {
  return element(`env-preview-${environment}`);
}

function previewInputValue(environment) {
  const field = previewTextArea(environment);
  return field ? field.value : "";
}

function resetEnvPreview(message, clearFields) {
  dataState.lastEnvPreview = null;
  hide("env-preview-result");
  hide("env-apply-panel");
  hide("env-apply-confirmation");
  if (clearFields) {
    ["development", "staging", "production"].forEach((environment) => {
      const field = previewTextArea(environment);
      if (field) {
        field.value = "";
      }
    });
  }
  showState("env-preview-state", message);
}

function previewCanApply(preview) {
  const environments = preview && Array.isArray(preview.environments) ? preview.environments : [];
  return Boolean(
    preview
      && !preview.applied
      && environments.length
      && environments.every((environment) => environment && environment.valid)
  );
}

function summaryMetric(label, count) {
  const item = document.createElement("div");
  const term = document.createElement("span");
  term.textContent = label;
  const value = document.createElement("strong");
  value.textContent = String(count);
  item.append(term, value);
  return item;
}

function appendPreviewEntries(container, title, entries) {
  if (!Array.isArray(entries) || !entries.length) {
    return;
  }
  const section = document.createElement("section");
  section.className = "env-preview-section";

  const heading = document.createElement("p");
  heading.className = "env-preview-section-title";
  heading.textContent = title;
  section.appendChild(heading);

  const list = document.createElement("ul");
  list.className = "env-preview-list";
  entries.forEach((entry) => {
    const item = document.createElement("li");
    item.textContent = `${text(entry.key)} • ${text(entry.before_masked)} -> ${text(entry.after_masked)}`;
    list.appendChild(item);
  });
  section.appendChild(list);
  container.appendChild(section);
}

function appendPreviewErrors(container, errors) {
  if (!Array.isArray(errors) || !errors.length) {
    return;
  }
  const section = document.createElement("section");
  section.className = "env-preview-section";

  const heading = document.createElement("p");
  heading.className = "env-preview-section-title";
  heading.textContent = "Errors";
  section.appendChild(heading);

  const list = document.createElement("ul");
  list.className = "env-preview-list errors";
  errors.forEach((entry) => {
    const item = document.createElement("li");
    item.textContent = `Line ${entry.line}: ${text(entry.reason)}`;
    list.appendChild(item);
  });
  section.appendChild(list);
  container.appendChild(section);
}

function previewSummaryCard(environment) {
  const card = document.createElement("article");
  card.className = "env-preview-card";

  const name = document.createElement("h3");
  name.textContent = text(environment.environment);
  card.appendChild(name);

  const summary = document.createElement("div");
  summary.className = "env-preview-summary";
  summary.appendChild(summaryMetric("Added", environment.added.length));
  summary.appendChild(summaryMetric("Updated", environment.updated.length));
  summary.appendChild(summaryMetric("Deleted", environment.deleted.length));
  summary.appendChild(summaryMetric("Errors", environment.errors.length));
  card.appendChild(summary);

  const validity = document.createElement("p");
  validity.className = `env-preview-validity${environment.valid ? " ok" : " warn"}`;
  validity.textContent = environment.valid ? "Valid preview." : "Preview invalid. Fix errors before retrying.";
  card.appendChild(validity);

  appendPreviewEntries(card, "Added keys", environment.added);
  appendPreviewEntries(card, "Updated keys", environment.updated);
  appendPreviewEntries(card, "Deleted keys", environment.deleted);
  appendPreviewErrors(card, environment.errors);

  if (Array.isArray(environment.unchanged) && environment.unchanged.length) {
    const details = document.createElement("details");
    details.className = "env-preview-details";
    const summaryLabel = document.createElement("summary");
    summaryLabel.textContent = `Unchanged (${environment.unchanged.length})`;
    details.appendChild(summaryLabel);
    appendPreviewEntries(details, "No effective masked change", environment.unchanged);
    card.appendChild(details);
  }

  return card;
}

function renderEnvPreviewResult(preview) {
  const result = element("env-preview-result");
  if (!result) {
    return;
  }
  clearChildren(result);

  const note = document.createElement("div");
  note.className = "env-preview-banner";

  const message = document.createElement("p");
  message.textContent = text(preview && preview.message, "Preview only. No changes have been saved.");
  note.appendChild(message);

  if (preview && preview.warning) {
    const warning = document.createElement("p");
    warning.textContent = preview.warning;
    note.appendChild(warning);
  }

  result.appendChild(note);

  const environments = preview && Array.isArray(preview.environments) ? preview.environments : [];
  environments.forEach((environment) => {
    result.appendChild(previewSummaryCard(environment));
  });

  dataState.lastEnvPreview = preview || null;
  if (previewCanApply(preview)) {
    show("env-apply-panel");
    hide("env-apply-confirmation");
  } else {
    hide("env-apply-panel");
    hide("env-apply-confirmation");
  }

  hideState("env-preview-state");
  show("env-preview-result");
}

function currentEnvChanges() {
  return {
    development: previewInputValue("development"),
    staging: previewInputValue("staging"),
    production: previewInputValue("production"),
  };
}

async function submitEnvPreview() {
  const project = selectedProject();
  if (!project) {
    resetEnvPreview("Select a project to preview masked environment changes.", false);
    return;
  }

  const button = element("env-preview-button");
  if (button) {
    button.disabled = true;
    button.textContent = "Previewing...";
  }
  hide("env-preview-result");
  showState("env-preview-state", "Previewing masked environment changes...");

  try {
    const preview = await fetchApiData(API_PATHS.projectEnvPreview(project.project_id), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        changes: currentEnvChanges(),
      }),
    });
    renderEnvPreviewResult(preview);
  } catch (error) {
    showState("env-preview-state", error.message || "Preview failed.", "warn");
    hide("env-apply-panel");
    hide("env-apply-confirmation");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "Preview Changes";
    }
  }
}

function renderAuditDiff(diff) {
  const wrapper = document.createElement("div");
  wrapper.className = "env-audit-diff";
  appendPreviewEntries(wrapper, "Masked diff", Array.isArray(diff) ? diff : []);
  return wrapper;
}

function renderEnvAuditHistory(audit) {
  const list = element("env-audit-history");
  if (!list) {
    return;
  }
  clearChildren(list);

  const total = audit && typeof audit.total === "number" ? audit.total : 0;
  setChip("env-audit-total-chip", `${total} events`, total ? "" : "stale");

  if (!audit || !Array.isArray(audit.entries) || !audit.entries.length) {
    hide("env-audit-history");
    showState("env-audit-state", "No masked audit history recorded for this project yet.");
    return;
  }

  audit.entries.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "env-audit-card";

    const head = document.createElement("div");
    head.className = "env-audit-head";
    const title = document.createElement("h3");
    title.textContent = `${text(entry.environment)} • ${text(entry.status)}`;
    const meta = document.createElement("p");
    meta.className = "env-audit-meta";
    meta.textContent = `Requested by ${text(entry.requested_by, "Unknown")} • ${formatUnix(entry.modified_at_unix)}`;
    head.append(title, meta);
    card.appendChild(head);

    const summary = document.createElement("div");
    summary.className = "env-preview-summary";
    summary.appendChild(summaryMetric("Added", entry.summary && entry.summary.added ? entry.summary.added : 0));
    summary.appendChild(summaryMetric("Updated", entry.summary && entry.summary.updated ? entry.summary.updated : 0));
    summary.appendChild(summaryMetric("Deleted", entry.summary && entry.summary.deleted ? entry.summary.deleted : 0));
    summary.appendChild(summaryMetric("Audit", text(entry.audit_id)));
    card.appendChild(summary);

    const details = document.createElement("details");
    details.className = "env-preview-details";
    const summaryLabel = document.createElement("summary");
    summaryLabel.textContent = "Show Diff";
    details.append(summaryLabel, renderAuditDiff(entry.diff));
    card.appendChild(details);

    list.appendChild(card);
  });

  hideState("env-audit-state");
  show("env-audit-history");
}

function toggleApplyConfirmation() {
  if (!previewCanApply(dataState.lastEnvPreview)) {
    return;
  }
  const confirmation = element("env-apply-confirmation");
  if (!confirmation) {
    return;
  }
  confirmation.hidden = !confirmation.hidden;
}

async function applyEnvChanges() {
  const project = selectedProject();
  if (!project || !previewCanApply(dataState.lastEnvPreview)) {
    return;
  }

  const button = element("env-apply-confirm-button");
  if (button) {
    button.disabled = true;
    button.textContent = "Applying...";
  }
  showState("env-preview-state", "Saving masked environment changes...");

  try {
    const response = await fetchApiData(API_PATHS.projectEnvApply(project.project_id), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        changes: currentEnvChanges(),
      }),
    });
    dataState.envInventoryByProject.delete(project.project_id);
    dataState.envAuditByProject.delete(project.project_id);
    hide("env-apply-confirmation");
    renderEnvPreviewResult(response);
    showState("env-preview-state", response.message || "Changes saved. They will apply on the next deployment.");
    const [inventory, audit] = await Promise.all([
      ensureProjectEnvInventory(project.project_id),
      ensureProjectEnvAudit(project.project_id),
    ]);
    renderEnvInventory(inventory);
    renderEnvAuditHistory(audit);
  } catch (error) {
    showState("env-preview-state", error.message || "Apply failed.", "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "Confirm Apply";
    }
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
      dataState.envInventoryByProject.clear();
      dataState.environmentsByProject.clear();
      dataState.envAuditByProject.clear();
      void loadConsole();
    });
  }

  const envSearch = element("env-key-search");
  if (envSearch) {
    envSearch.addEventListener("input", (event) => {
      uiState.envQuery = event.target.value || "";
      renderEnvInventory(dataState.envInventoryByProject.get(uiState.selectedProjectId) || null);
    });
  }

  const previewButton = element("env-preview-button");
  if (previewButton) {
    previewButton.addEventListener("click", () => {
      void submitEnvPreview();
    });
  }

  const applyButton = element("env-apply-button");
  if (applyButton) {
    applyButton.addEventListener("click", toggleApplyConfirmation);
  }

  const applyConfirmButton = element("env-apply-confirm-button");
  if (applyConfirmButton) {
    applyConfirmButton.addEventListener("click", () => {
      void applyEnvChanges();
    });
  }
}

bindControls();
void loadConsole();
