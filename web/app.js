const API_PATHS = {
  readyz: "/readyz",
  metrics: "/metrics",
  explain: "/readiness/explain",
  timeline: "/readiness/timeline",
  githubRepos: "/api/github/repos",
  projects: "/api/projects",
  projectRegisterFromGitHubPreview: "/api/projects/register-from-github/preview",
  projectRegisterFromGitHub: "/api/projects/register-from-github",
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
  projectDeployPreview(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/deploy/preview`;
  },
  projectDeploy(projectId) {
    return `/api/projects/${encodeURIComponent(projectId)}/deploy`;
  },
  deploymentStatus(deploymentId) {
    return `/deployments/${encodeURIComponent(deploymentId)}`;
  },
  deploymentLogs(deploymentId) {
    return `/api/deployments/${encodeURIComponent(deploymentId)}/logs`;
  },
  environmentStatus(projectId, environment) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments/${encodeURIComponent(environment)}`;
  },
  environmentDiagnostics(projectId, environment) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments/${encodeURIComponent(environment)}/diagnostics`;
  },
  environmentDeployments(projectId, environment) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments/${encodeURIComponent(environment)}/deployments`;
  },
  environmentGenerations(projectId, environment) {
    return `/api/projects/${encodeURIComponent(projectId)}/environments/${encodeURIComponent(environment)}/generations`;
  },
};

const uiState = {
  query: "",
  selectedProjectId: "",
  githubRepoQuery: "",
  envQuery: "",
  envAuditEnvironment: "all",
  envAuditStatus: "all",
};

const dataState = {
  readyz: null,
  metrics: null,
  explain: null,
  timeline: null,
  projects: [],
  githubRepos: [],
  githubRepoListLoaded: false,
  selectedGithubRepo: null,
  githubRegistrationPreview: null,
  environmentsByProject: new Map(),
  envInventoryByProject: new Map(),
  envAuditByProject: new Map(),
  deploymentHistoryByEnvironment: new Map(),
  generationTruthByEnvironment: new Map(),
  lastEnvPreview: null,
  lastEnvPreviewSignature: "",
  lastEnvApplyIdempotencyKey: "",
  envApplyInFlight: false,
  envPreviewPhase: "no_preview",
  deployPreview: null,
  deployPreviewSignature: "",
  deployPreviewValid: false,
  deployInFlight: false,
  deployTrackingTimer: null,
  deployTracking: null,
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

async function fetchJson(path, options = {}) {
  const init = {
    headers: { Accept: "application/json" },
    credentials: "same-origin",
    ...options,
  };
  init.headers = {
    Accept: "application/json",
    ...(options.headers || {}),
  };
  const response = await fetch(path, init);
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const error = new Error((payload && payload.message) || `request failed: ${response.status}`);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
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
  if (health === "not_deployed" || health === "unavailable" || health === "missing" || health === "unknown") {
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

function setPreviewPhase(phase) {
  dataState.envPreviewPhase = phase || "no_preview";
  const node = element("env-preview-phase-chip");
  if (!node) {
    return;
  }
  const labels = {
    no_preview: ["No preview yet", "stale"],
    preview_valid: ["Preview valid", "ok"],
    preview_has_errors: ["Preview has errors", "warn"],
    preview_stale: ["Preview stale", "warn"],
    applying: ["Applying", "warn"],
    applied: ["Applied", "ok"],
    apply_failed: ["Apply failed", "warn"],
    idempotent_replay: ["Idempotent replay", "ok"],
  };
  const [label, tone] = labels[phase] || [text(phase), ""];
  setChip("env-preview-phase-chip", label, tone);
}

function selectedDeployEnvironment() {
  const field = element("deploy-environment-select");
  return field ? (field.value || "staging") : "staging";
}

function selectedTruthEnvironment() {
  const field = element("history-environment-select");
  return field ? (field.value || "staging") : "staging";
}

function deployRefValue() {
  const field = element("deploy-ref-input");
  return field ? (field.value || "").trim() : "";
}

function deployPreviewSignature(projectId, environment, gitRef) {
  return `${projectId}::${environment}::${gitRef}`;
}

function clearDeployTrackingTimer() {
  if (dataState.deployTrackingTimer) {
    window.clearTimeout(dataState.deployTrackingTimer);
    dataState.deployTrackingTimer = null;
  }
}

function resetDeployCenterState(message, tone = "stale") {
  dataState.deployPreview = null;
  dataState.deployPreviewValid = false;
  dataState.deployPreviewSignature = "";
  if (!dataState.deployInFlight) {
    clearDeployTrackingTimer();
    dataState.deployTracking = null;
  }
  setChip("deploy-center-chip", tone === "warn" ? "Needs review" : tone === "ok" ? "Ready" : "Idle", tone);
  showState("deploy-center-state", message, tone === "ok" ? "" : tone);
  hide("deploy-preview-result");
  if (!dataState.deployTracking) {
    hide("deploy-tracking");
  }
  const confirmButton = element("deploy-confirm-button");
  if (confirmButton) {
    confirmButton.disabled = true;
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

function githubRepoList() {
  return Array.isArray(dataState.githubRepos) ? dataState.githubRepos : [];
}

function filteredGithubRepos() {
  const query = lower(uiState.githubRepoQuery);
  if (!query) {
    return githubRepoList();
  }

  return githubRepoList().filter((repository) => {
    const haystacks = [
      repository.full_name,
      repository.clone_url,
      repository.default_branch,
      repository.private ? "private" : "public",
    ];
    return haystacks.some((value) => lower(value).includes(query));
  });
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

function setGitHubRegisterState(message, tone = "") {
  showState("github-repo-state", message, tone);
  setChip(
    "github-register-chip",
    tone === "warn" ? "Needs review" : dataState.githubRepoListLoaded ? "Ready" : "Idle",
    tone || (dataState.githubRepoListLoaded ? "ok" : "stale"),
  );
}

function githubPreviewIsRegisterable(preview) {
  return Boolean(
    preview
    && preview.valid
    && preview.project_id_status === "valid"
    && preview.base_domain_status === "available",
  );
}

function githubProjectIdInputValue() {
  const field = element("github-project-id-input");
  return field ? (field.value || "").trim() : "";
}

function githubBaseDomainInputValue() {
  const field = element("github-base-domain-input");
  return field ? (field.value || "").trim() : "";
}

function githubProjectIdConfirmed() {
  const field = element("github-project-id-confirm");
  return Boolean(field && field.checked);
}

function resetGitHubProjectConfirmation() {
  const field = element("github-project-id-confirm");
  if (field) {
    field.checked = false;
  }
}

function renderGitHubProjectIdAlternatives(preview) {
  const list = element("github-project-id-alternatives");
  if (!list) {
    return;
  }
  clearChildren(list);
  const alternatives = preview && Array.isArray(preview.project_id_alternatives)
    ? preview.project_id_alternatives.filter((value) => value && value !== preview.project_id)
    : [];
  if (!alternatives.length) {
    hide("github-project-id-alternatives");
    return;
  }

  alternatives.forEach((projectId) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "project-item";
    button.textContent = projectId;
    button.addEventListener("click", () => {
      const field = element("github-project-id-input");
      if (field) {
        field.value = projectId;
      }
      resetGitHubProjectConfirmation();
      void refreshGitHubRegistrationPreview();
    });
    list.appendChild(button);
  });
  show("github-project-id-alternatives");
}

function renderGitHubRegistrationMessages(preview) {
  const container = element("github-register-messages");
  if (!container) {
    return;
  }
  clearChildren(container);
  const warnings = preview && Array.isArray(preview.warnings) ? preview.warnings : [];
  const errors = preview && Array.isArray(preview.errors) ? preview.errors : [];
  if (!warnings.length && !errors.length) {
    hide("github-register-messages");
    return;
  }

  warnings.forEach((message) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = `Warning: ${message}`;
    container.appendChild(item);
  });
  errors.forEach((message) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = `Error: ${message}`;
    container.appendChild(item);
  });
  show("github-register-messages");
}

function syncGitHubPreviewInputs(preview) {
  const projectIdField = element("github-project-id-input");
  if (projectIdField && projectIdField.value.trim() !== preview.project_id) {
    projectIdField.value = preview.project_id || "";
  }

  const baseDomainField = element("github-base-domain-input");
  if (baseDomainField) {
    if (preview.domain_source === "explicit") {
      baseDomainField.value = preview.base_domain || "";
    } else if (baseDomainField.value.trim()) {
      baseDomainField.value = "";
    }
  }
}

function updateGitHubRegisterButton(preview) {
  const button = element("github-register-button");
  if (!button) {
    return;
  }
  button.disabled = !githubPreviewIsRegisterable(preview) || !githubProjectIdConfirmed();
}

function renderGitHubRegistrationPreview() {
  const repository = dataState.selectedGithubRepo;
  const preview = dataState.githubRegistrationPreview;
  if (!repository || !preview) {
    hide("github-register-preview");
    return;
  }

  element("github-selected-full-name").textContent = text(repository.full_name);
  element("github-selected-default-branch").textContent = text(preview.default_branch);
  element("github-selected-clone-url").textContent = text(preview.repo_url);
  element("github-selected-project-id").textContent = text(preview.project_id);
  element("github-selected-base-domain").textContent = text(preview.base_domain);
  element("github-selected-domain-source").textContent = text(preview.domain_source);
  element("github-project-id-status").textContent = text(preview.project_id_status);
  element("github-base-domain-status").textContent = text(preview.base_domain_status);
  element("github-project-id-message").textContent = text(preview.project_id_message, "Choose and confirm a valid final project ID.");
  element("github-base-domain-message").textContent = text(
    preview.base_domain_message || preview.base_domain_suggestion,
    "Production uses the base domain directly. Staging and development routes are derived automatically.",
  );
  setChip("github-repo-visibility", repository.private ? "Private repo" : "Public repo", repository.private ? "warn" : "");
  element("github-route-production").textContent = text(preview.environment_routes && preview.environment_routes.production);
  element("github-route-staging").textContent = text(preview.environment_routes && preview.environment_routes.staging);
  element("github-route-development").textContent = text(preview.environment_routes && preview.environment_routes.development);
  syncGitHubPreviewInputs(preview);
  renderGitHubProjectIdAlternatives(preview);
  renderGitHubRegistrationMessages(preview);
  updateGitHubRegisterButton(preview);
  show("github-register-preview");
}

function renderGitHubRepoList() {
  const list = element("github-repo-list");
  if (!list) {
    return;
  }
  clearChildren(list);

  if (!dataState.githubRepoListLoaded) {
    hide("github-repo-list");
    hide("github-register-preview");
    return;
  }

  const repositories = filteredGithubRepos();
  if (!githubRepoList().length) {
    hide("github-repo-list");
    hide("github-register-preview");
    return;
  }

  if (!repositories.length) {
    hide("github-repo-list");
    hide("github-register-preview");
    setGitHubRegisterState("No repository matches that search.", "warn");
    return;
  }

  repositories.forEach((repository) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `project-item${dataState.selectedGithubRepo && dataState.selectedGithubRepo.full_name === repository.full_name ? " is-active" : ""}`;
    button.addEventListener("click", () => {
      void selectGitHubRepository(repository);
    });

    const titleRow = document.createElement("div");
    titleRow.className = "project-item-row";

    const title = document.createElement("p");
    title.className = "project-item-title";
    title.textContent = repository.full_name;

    const chip = document.createElement("span");
    chip.className = `status-chip${repository.private ? " warn" : ""}`;
    chip.textContent = repository.private ? "Private" : "Public";
    titleRow.append(title, chip);

    const meta = document.createElement("p");
    meta.className = "project-item-meta";
    meta.textContent = repository.html_url;

    const note = document.createElement("p");
    note.className = "project-item-note";
    note.textContent = `Branch ${text(repository.default_branch)} • ${text(repository.clone_url)}`;

    button.append(titleRow, meta, note);
    list.appendChild(button);
  });

  hideState("github-repo-state");
  show("github-repo-list");
  renderGitHubRegistrationPreview();
}

async function loadGitHubRepositories() {
  const button = element("github-repo-refresh");
  if (button) {
    button.disabled = true;
    button.textContent = "Loading...";
  }
  hide("github-register-preview");
  setGitHubRegisterState("Loading accessible GitHub repositories...");

  try {
    const payload = await fetchApiData(API_PATHS.githubRepos);
    dataState.githubRepos = Array.isArray(payload.repositories) ? payload.repositories : [];
    dataState.githubRepoListLoaded = true;
    dataState.selectedGithubRepo = null;
    dataState.githubRegistrationPreview = null;
    renderGitHubRepoList();
    if (payload.message) {
      setGitHubRegisterState(payload.message, payload.private_repo_authorization_required ? "warn" : "");
    } else if (!dataState.githubRepos.length) {
      setGitHubRegisterState("No accessible GitHub repositories were found for this account.", "warn");
    } else {
      setGitHubRegisterState("Select a repository to preview project registration.");
    }
  } catch (error) {
    dataState.githubRepos = [];
    dataState.githubRepoListLoaded = false;
    dataState.selectedGithubRepo = null;
    dataState.githubRegistrationPreview = null;
    hide("github-repo-list");
    hide("github-register-preview");
    setGitHubRegisterState(error.message || "GitHub repository listing is unavailable right now.", "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "Load Repositories";
    }
  }
}

async function refreshGitHubRegistrationPreview() {
  const repository = dataState.selectedGithubRepo;
  if (!repository) {
    setGitHubRegisterState("Select a repository first.", "warn");
    return;
  }
  resetGitHubProjectConfirmation();
  setGitHubRegisterState("Loading registration preview...");

  try {
    const preview = await fetchApiData(API_PATHS.projectRegisterFromGitHubPreview, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        repo_url: repository.clone_url,
        default_branch: repository.default_branch,
        project_id: githubProjectIdInputValue() || null,
        base_domain: githubBaseDomainInputValue() || null,
      }),
    });
    dataState.githubRegistrationPreview = preview;
    renderGitHubRepoList();
    setGitHubRegisterState(
      githubPreviewIsRegisterable(preview)
        ? "Confirm the final project_id to register this project only."
        : "Review validation errors before registering.",
      githubPreviewIsRegisterable(preview) ? "" : "warn",
    );
  } catch (error) {
    dataState.githubRegistrationPreview = null;
    hide("github-register-preview");
    setGitHubRegisterState(error.message || "Registration preview failed.", "warn");
  }
}

async function selectGitHubRepository(repository) {
  dataState.selectedGithubRepo = repository;
  dataState.githubRegistrationPreview = null;
  resetGitHubProjectConfirmation();
  const projectIdField = element("github-project-id-input");
  if (projectIdField) {
    projectIdField.value = "";
  }
  const baseDomainField = element("github-base-domain-input");
  if (baseDomainField) {
    baseDomainField.value = "";
  }
  renderGitHubRepoList();
  await refreshGitHubRegistrationPreview();
}

async function registerProjectFromGitHub() {
  const preview = dataState.githubRegistrationPreview;
  if (!preview) {
    setGitHubRegisterState("Select a repository and wait for registration preview first.", "warn");
    return;
  }
  if (!githubPreviewIsRegisterable(preview)) {
    setGitHubRegisterState("Preview is not valid yet. Fix validation errors before registering.", "warn");
    return;
  }
  if (!githubProjectIdConfirmed()) {
    setGitHubRegisterState("Confirm the final project_id before registering.", "warn");
    return;
  }

  const button = element("github-register-button");
  if (button) {
    button.disabled = true;
    button.textContent = "Registering...";
  }
  setGitHubRegisterState("Registering project without deployment...");

  try {
    const response = await fetchApiData(API_PATHS.projectRegisterFromGitHub, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        project_id: preview.project_id,
        repo_url: preview.repo_url,
        default_branch: preview.default_branch,
        base_domain: githubBaseDomainInputValue() || null,
      }),
    });
    setGitHubRegisterState("Project registered. Deploy from CLI/API when ready.");
    dataState.githubRegistrationPreview = {
      ...preview,
      project_id: response.project_id,
      repo_url: response.repo_url,
      default_branch: response.default_branch,
      base_domain: response.base_domain,
      domain_source: response.domain_source,
      environment_routes: response.environment_routes,
      valid: true,
      project_id_status: "valid",
      base_domain_status: "available",
      warnings: [],
      errors: [],
    };
    resetGitHubProjectConfirmation();
    uiState.selectedProjectId = response.project_id;
    dataState.environmentsByProject.delete(response.project_id);
    dataState.envInventoryByProject.delete(response.project_id);
    dataState.envAuditByProject.delete(response.project_id);
    dataState.deploymentHistoryByEnvironment.clear();
    dataState.generationTruthByEnvironment.clear();
    renderGitHubRegistrationPreview();
    await loadConsole();
  } catch (error) {
    setGitHubRegisterState(error.message || "Project registration failed.", "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "Register Project";
    }
    updateGitHubRegisterButton(dataState.githubRegistrationPreview);
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

function truthCacheKey(projectId, environment) {
  return `${projectId}::${environment}`;
}

async function ensureEnvironmentDeployments(projectId, environment) {
  if (!projectId || !environment) {
    return null;
  }
  const key = truthCacheKey(projectId, environment);
  if (dataState.deploymentHistoryByEnvironment.has(key)) {
    return dataState.deploymentHistoryByEnvironment.get(key) || null;
  }

  try {
    const payload = await fetchApiData(API_PATHS.environmentDeployments(projectId, environment));
    dataState.deploymentHistoryByEnvironment.set(key, payload);
    return payload;
  } catch (_error) {
    throw new Error("Deployment history unavailable.");
  }
}

async function ensureEnvironmentGenerations(projectId, environment) {
  if (!projectId || !environment) {
    return null;
  }
  const key = truthCacheKey(projectId, environment);
  if (dataState.generationTruthByEnvironment.has(key)) {
    return dataState.generationTruthByEnvironment.get(key) || null;
  }

  try {
    const payload = await fetchApiData(API_PATHS.environmentGenerations(projectId, environment));
    dataState.generationTruthByEnvironment.set(key, payload);
    return payload;
  } catch (_error) {
    throw new Error("Generation truth unavailable.");
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

function setSelectOptions(id, environments, selectedValue) {
  const field = element(id);
  if (!field) {
    return;
  }
  const values = Array.isArray(environments) && environments.length
    ? environments
    : ["staging", "production", "development"];
  clearChildren(field);
  values.forEach((environment) => {
    const option = document.createElement("option");
    option.value = environment;
    option.textContent = environment;
    if (environment === selectedValue) {
      option.selected = true;
    }
    field.appendChild(option);
  });
}

function truthCard(title, subtitle, tone = "") {
  const card = document.createElement("article");
  card.className = "truth-card";

  const head = document.createElement("div");
  head.className = "truth-card-head";

  const copy = document.createElement("div");
  const name = document.createElement("p");
  name.className = "truth-card-title";
  name.textContent = title;
  const note = document.createElement("p");
  note.className = "truth-card-subtitle";
  note.textContent = subtitle;
  copy.append(name, note);
  head.appendChild(copy);

  if (tone) {
    const chip = document.createElement("span");
    chip.className = `status-chip ${tone}`;
    chip.textContent = tone === "ok" ? "Clear" : tone === "warn" ? "Review" : "Partial";
    head.appendChild(chip);
  }

  card.appendChild(head);
  return card;
}

function appendTruthFacts(card, facts) {
  const list = document.createElement("dl");
  list.className = "truth-facts";
  facts.forEach(([label, value]) => {
    appendMeta(list, label, value);
  });
  card.appendChild(list);
}

function appendTruthCopy(card, message) {
  const copy = document.createElement("p");
  copy.className = "truth-copy";
  copy.textContent = message;
  card.appendChild(copy);
}

function routeTruthTone(routeTruth) {
  if (routeTruth && routeTruth.fallback_detected === true) {
    return "warn";
  }
  if (routeTruth && routeTruth.route_expected && routeTruth.route_active === true && routeTruth.app_route_healthy === true) {
    return "ok";
  }
  if (routeTruth && routeTruth.route_active === false) {
    return "warn";
  }
  return "stale";
}

function renderRouteTruth(routeTruth) {
  const container = element("route-truth-result");
  if (!container) {
    return;
  }
  clearChildren(container);
  const truth = routeTruth || {};
  const tone = routeTruthTone(truth);
  const card = truthCard(
    text(truth.domain, "Route not assigned"),
    truth.fallback_detected === true ? "Gateway fallback" : "Cached route truth",
    tone,
  );
  appendTruthFacts(card, [
    ["Expected route", truth.route_expected ? text(truth.domain, "Unknown") : "No HTTP route required"],
    ["Route active", truth.route_active === undefined || truth.route_active === null ? "Unknown" : truth.route_active ? "true" : "false"],
    ["Fallback detected", truth.fallback_detected === undefined || truth.fallback_detected === null ? "Unknown" : truth.fallback_detected ? "true" : "false"],
    ["App route healthy", truth.app_route_healthy === undefined || truth.app_route_healthy === null ? "Unknown" : truth.app_route_healthy ? "true" : "false"],
  ]);
  appendTruthCopy(
    card,
    truth.fallback_detected === true
      ? "Gateway fallback. Application route is not active. Gateway healthy is not app healthy."
      : text(truth.detail, "Partial metadata available."),
  );
  container.appendChild(card);
}

function renderRollbackEligibility(rollback) {
  const container = element("rollback-eligibility-result");
  if (!container) {
    return;
  }
  clearChildren(container);
  const item = rollback || { state: "unknown", message: "Unknown: metadata unavailable" };
  const tone = item.state === "eligible" ? "ok" : item.state === "not_eligible" ? "warn" : "stale";
  const card = truthCard(
    item.state === "eligible" ? "Rollback target retained" : "Rollback not available",
    text(item.message),
    tone,
  );
  appendTruthFacts(card, [
    ["State", text(item.state)],
    ["Generation", item.generation === undefined || item.generation === null ? "None" : `Gen ${item.generation}`],
  ]);
  appendTruthCopy(card, "Read-only only. No rollback button is exposed yet.");
  container.appendChild(card);
}

function renderDeploymentHistory(deployments) {
  const container = element("deployment-history-result");
  if (!container) {
    return;
  }
  clearChildren(container);
  const entries = deployments && Array.isArray(deployments.entries) ? deployments.entries : [];
  setChip("deployment-history-count-chip", `${entries.length} ${entries.length === 1 ? "entry" : "entries"}`, entries.length ? "" : "stale");
  if (!entries.length) {
    const card = truthCard("No deployments recorded", "No deployment attempts are stored for this environment yet.", "stale");
    appendTruthCopy(card, "No deployments recorded.");
    container.appendChild(card);
    return;
  }

  entries.forEach((entry) => {
    const tone = lower(entry.status_label) === "live" ? "ok" : entry.failure_reason ? "warn" : "stale";
    const card = truthCard(
      `${text(entry.deployment_id, `Generation ${entry.generation}`)}`,
      `Gen ${entry.generation} • ${text(entry.status_label)}`,
      tone,
    );
    appendTruthFacts(card, [
      ["State", text(entry.state)],
      ["Lifecycle", text(entry.lifecycle_state)],
      ["Source ref", text(entry.source_ref, "Unknown")],
      ["Commit", text(entry.commit_sha, "Unknown")],
      ["Started", formatUnix(entry.started_at_unix)],
      ["Completed", formatUnix(entry.completed_at_unix)],
      ["Route", text(entry.route, "Unknown")],
      ["Route active", entry.route_active === undefined || entry.route_active === null ? "Unknown" : entry.route_active ? "true" : "false"],
      ["Health", text(entry.health_summary, "Unknown")],
      ["Failure stage", text(entry.failure_stage, "None")],
      ["Failure reason", text(entry.failure_reason, "None")],
      ["Live URL safe", entry.safe_to_report_live_url === undefined || entry.safe_to_report_live_url === null ? "Unknown" : entry.safe_to_report_live_url ? "true" : "false"],
    ]);
    appendTruthCopy(card, `Next action: ${text(entry.recommended_next_action)}`);
    container.appendChild(card);
  });
}

function renderGenerationTruth(generations) {
  const container = element("generation-truth-result");
  if (!container) {
    return;
  }
  clearChildren(container);
  const entries = generations && Array.isArray(generations.entries) ? generations.entries : [];
  setChip("generation-truth-count-chip", `${entries.length} ${entries.length === 1 ? "generation" : "generations"}`, entries.length ? "" : "stale");
  if (!entries.length) {
    const card = truthCard("No active generation", "No generations are retained for this environment yet.", "stale");
    appendTruthCopy(card, "No active generation.");
    container.appendChild(card);
    return;
  }

  entries.forEach((entry) => {
    const roles = Array.isArray(entry.roles) && entry.roles.length ? entry.roles.join(", ") : "retained";
    const tone = Array.isArray(entry.roles) && entry.roles.includes("current")
      ? "ok"
      : Array.isArray(entry.roles) && entry.roles.includes("failed")
        ? "warn"
        : "stale";
    const card = truthCard(`Generation ${entry.generation}`, roles, tone);
    appendTruthFacts(card, [
      ["Roles", roles],
      ["Lifecycle", text(entry.lifecycle)],
      ["Route state", text(entry.route_state)],
      ["Services", String(entry.service_count || 0)],
      ["Runtime policy", text(entry.runtime_policy_summary, "Unknown")],
      ["Created", formatUnix(entry.created_at_unix)],
      ["Finalized", formatUnix(entry.finalized_at_unix)],
      ["Promoted", formatUnix(entry.promoted_at_unix)],
      ["Failure reason", text(entry.failure_reason, "None")],
      ["Env snapshot", entry.env_snapshot ? (entry.env_snapshot.exists ? "true" : "false") : "Unknown"],
      ["Env key count", entry.env_snapshot && entry.env_snapshot.key_count !== undefined && entry.env_snapshot.key_count !== null ? String(entry.env_snapshot.key_count) : "Unknown"],
      ["Env source", entry.env_snapshot ? text(entry.env_snapshot.source) : "Unknown"],
    ]);
    if (entry.env_snapshot) {
      appendTruthCopy(card, text(entry.env_snapshot.copy));
    } else {
      appendTruthCopy(card, "Partial metadata available.");
    }
    container.appendChild(card);
  });
}

async function renderDeploymentTruth(projectId, environments) {
  const selectedEnvironment = selectedTruthEnvironment();
  setSelectOptions("history-environment-select", environments, selectedEnvironment);
  const environment = selectedTruthEnvironment();
  const state = element("deployment-truth-state");
  if (!projectId) {
    setChip("deployment-truth-chip", "Idle", "stale");
    showState("deployment-truth-state", "Select a project and environment to inspect deployment truth.");
    hide("deployment-truth-panels");
    return;
  }
  showState("deployment-truth-state", "Loading deployment truth...");
  hide("deployment-truth-panels");

  try {
    const [deployments, generations] = await Promise.all([
      ensureEnvironmentDeployments(projectId, environment),
      ensureEnvironmentGenerations(projectId, environment),
    ]);
    renderRouteTruth((deployments && deployments.route_truth) || (generations && generations.route_truth) || null);
    renderRollbackEligibility((deployments && deployments.rollback_eligibility) || (generations && generations.rollback_eligibility) || null);
    renderDeploymentHistory(deployments);
    renderGenerationTruth(generations);
    hideState("deployment-truth-state");
    show("deployment-truth-panels");
    const routeTruth = (deployments && deployments.route_truth) || (generations && generations.route_truth) || null;
    setChip(
      "deployment-truth-chip",
      routeTruth && routeTruth.fallback_detected === true ? "Fallback" : "Read-only",
      routeTruth && routeTruth.fallback_detected === true ? "warn" : "ok",
    );
  } catch (error) {
    setChip("deployment-truth-chip", "Unavailable", "stale");
    showState("deployment-truth-state", error.message || "Deployment truth unavailable.", "warn");
    hide("deployment-truth-panels");
  }
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
  route.textContent = text(environment.route, "Route not assigned");
  title.append(name, route);

  const chip = document.createElement("span");
  chip.className = `status-chip${tone ? ` ${tone}` : ""}`;
  const healthState = lower(environment.readiness_summary && environment.readiness_summary.health_state);
  chip.textContent = healthState === "not_deployed"
    ? "Not deployed"
    : healthState === "degraded" && lower(environment.route).includes("gateway fallback")
      ? "Gateway fallback"
      : text(
          environment.readiness_summary && environment.readiness_summary.health_state,
          environment.last_deployment_status || "Unknown",
        );

  header.append(title, chip);
  card.appendChild(header);

  const facts = document.createElement("dl");
  facts.className = "environment-facts";
  appendMeta(facts, "Current", environment.current_generation === null || environment.current_generation === undefined ? "None" : `Gen ${environment.current_generation}`);
  appendMeta(facts, "Previous", environment.previous_generation === null || environment.previous_generation === undefined ? "None" : `Gen ${environment.previous_generation}`);
  appendMeta(
    facts,
    "Deploy",
    lower(environment.last_deployment_status) === "not_deployed"
      ? "Not deployed"
      : text(environment.last_deployment_status),
  );
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
  note.textContent = reasons.length
    ? reasons[0]
    : lower(environment.route).includes("gateway fallback")
      ? "Gateway is reachable, app route is not serving yet."
      : "No additional readiness notes.";
  card.appendChild(note);

  return card;
}

function envColumnNames(inventory) {
  const environments = inventory && Array.isArray(inventory.environments) ? inventory.environments : [];
  return environments.length ? environments : ["development", "staging", "production"];
}

function setEnvSourceMeta(inventory) {
  const wrapper = element("env-inventory-meta");
  if (!wrapper) {
    return;
  }
  clearChildren(wrapper);
  const note = inventory && inventory.partial_metadata_note
    ? inventory.partial_metadata_note
    : inventory && inventory.partial_metadata_notice
      ? inventory.partial_metadata_notice
      : "Masked values only.";
  const title = document.createElement("p");
  title.id = "env-source-label";
  title.className = "env-source-label";
  title.textContent = inventory ? text(inventory.source_label) : "Unknown source";
  wrapper.appendChild(title);

  const detail = document.createElement("p");
  detail.id = "env-source-note";
  detail.className = "env-source-note";
  detail.textContent = note;
  wrapper.appendChild(detail);

  const copy = document.createElement("p");
  copy.className = "env-source-note";
  copy.textContent = "Applies on next deployment. Current running generation is unchanged. Rollback uses sealed historical snapshots.";
  wrapper.appendChild(copy);

  const sources = inventory && Array.isArray(inventory.environment_sources) ? inventory.environment_sources : [];
  if (sources.length) {
    const grid = document.createElement("div");
    grid.className = "env-source-grid";
    sources.forEach((source) => {
      const card = document.createElement("article");
      card.className = "env-source-card";

      const name = document.createElement("h3");
      name.textContent = text(source.environment);
      card.appendChild(name);

      const facts = document.createElement("dl");
      facts.className = "env-source-facts";
      appendMeta(facts, "Source", text(source.source_label));
      appendMeta(facts, "Revision", text(source.revision_label || `Revision ${source.env_store_revision}`, "Revision 0"));
      appendMeta(facts, "Last updated", formatUnix(source.updated_at_unix));
      appendMeta(facts, "Updated by", text(source.updated_by, "Unknown"));
      card.appendChild(facts);
      grid.appendChild(card);
    });
    wrapper.appendChild(grid);
  }
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
        : { exists: false, value: "not configured", value_state: "missing" };

      const presence = document.createElement("div");
      presence.className = `env-presence ${cell.exists ? "exists" : "missing"}`;
      const dot = document.createElement("span");
      dot.className = "env-presence-dot";
      const label = document.createElement("span");
      label.textContent = cell.exists ? "present" : "not configured";
      presence.append(dot, label);

      const value = document.createElement("p");
      value.className = `env-value${cell.exists ? "" : " missing"}`;
      value.textContent = text(cell.value, "not configured");
      wrapper.append(presence, value);

      const statusRow = document.createElement("div");
      statusRow.className = "env-inline-badges";
      if (cell.pending_next_deploy) {
        const badge = document.createElement("span");
        badge.className = "status-chip warn";
        badge.textContent = "pending next deploy";
        statusRow.appendChild(badge);
      } else if (cell.matches_deployed) {
        const badge = document.createElement("span");
        badge.className = "status-chip";
        badge.textContent = "matches deployed";
        statusRow.appendChild(badge);
      }
      if (statusRow.childNodes.length) {
        wrapper.appendChild(statusRow);
      }

      if (cell.next_deploy_label !== undefined && cell.next_deploy_label !== null) {
        const configured = document.createElement("p");
        configured.className = "env-value env-detail-line";
        configured.textContent = `Next deploy: ${text(cell.next_deploy_label, "not configured")}`;
        wrapper.appendChild(configured);
      }
      if (cell.deployed_label !== undefined && cell.deployed_label !== null) {
        const deployed = document.createElement("p");
        deployed.className = "env-value env-detail-line";
        deployed.textContent = `Last deployed: ${text(cell.deployed_label, "not configured")}`;
        wrapper.appendChild(deployed);
      }
      if (!cell.configured_exists && !cell.pending_next_deploy && cell.deployed_exists) {
        const note = document.createElement("p");
        note.className = "env-value missing";
        note.textContent = "No configured override exists.";
        wrapper.appendChild(note);
      }

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
    await renderDeploymentTruth("", []);
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
      await renderDeploymentTruth(project.project_id, []);
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
    await renderDeploymentTruth(
      project.project_id,
      environments.map((entry) => entry.environment),
    );
  } catch (error) {
    showState("project-detail-state", error.message || "Environment inventory unavailable.", "warn");
    showState("env-inventory-state", error.message || "Env inventory unavailable.", "warn");
    setChip("project-health-chip", "Unavailable", "stale");
    setChip("environment-count-chip", "0 env", "stale");
    setChip("env-total-chip", "0 vars", "stale");
    showState("env-preview-state", "Preview unavailable until project inventory loads.", "warn");
    showState("env-audit-state", error.message || "Audit history unavailable.", "warn");
    await renderDeploymentTruth(project.project_id, []);
  }

  syncDeployCenterForSelectedProject();
}

function syncDeployCenterForSelectedProject() {
  const project = selectedProject();
  const projectField = element("deploy-project-id");
  if (projectField) {
    projectField.value = project ? project.project_id : "";
  }
  if (!project) {
    resetDeployCenterState("Select a project, choose an environment, enter a Git ref, then run preflight.");
    return;
  }
  if (dataState.deployPreviewSignature) {
    const expected = deployPreviewSignature(project.project_id, selectedDeployEnvironment(), deployRefValue());
    if (expected !== dataState.deployPreviewSignature) {
      resetDeployCenterState("Project, environment, or ref changed. Run preflight again.", "warn");
    }
  } else {
    resetDeployCenterState("Choose an environment and Git ref, then run preflight.");
  }
}

function renderDeployPreview(preview) {
  const container = element("deploy-preview-result");
  if (!container) {
    return;
  }
  clearChildren(container);

  const lines = [
    `Project: ${text(preview.project_id)}`,
    `Environment: ${text(preview.environment)}`,
    `Repository: ${text(preview.repo_url)}`,
    `Ref: ${text(preview.git_ref)}`,
    `Commit: ${text(preview.commit_sha, "unresolved")}`,
    `Route: ${text(preview.route && preview.route.domain)}`,
    `Services: ${(preview.manifest && preview.manifest.services || []).join(", ") || "none"}`,
    `Exposed services: ${(preview.manifest && preview.manifest.exposed_services || []).join(", ") || "none"}`,
    `Healthchecks: ${(preview.manifest && preview.manifest.healthchecks || []).map((entry) => `${entry.service_id}:${entry.path}`).join(", ") || "none"}`,
    `Pending desired env: ${preview.env && preview.env.pending_desired_env ? "yes" : "no"}`,
  ];

  lines.forEach((line) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = line;
    container.appendChild(item);
  });

  if (preview.env && Array.isArray(preview.env.missing_required_secrets) && preview.env.missing_required_secrets.length) {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = `Missing required secrets: ${preview.env.missing_required_secrets.join(", ")}`;
    container.appendChild(item);
  }

  (preview.warnings || []).forEach((message) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = `Warning: ${message}`;
    container.appendChild(item);
  });
  (preview.errors || []).forEach((message) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = `Error: ${message}`;
    container.appendChild(item);
  });

  show("deploy-preview-result");
}

function deploymentStage(status, environmentStatus, diagnostics) {
  const raw = lower(status && status.state);
  if (raw === "queued") {
    return "queued";
  }
  if (raw === "building") {
    return "building";
  }
  if (raw === "starting" || raw === "active") {
    return "preparing";
  }
  const lifecycle = lower(environmentStatus && environmentStatus.lifecycle_state);
  if (lifecycle === "warming" || raw === "warming") {
    return "warming";
  }
  if (lifecycle === "validating" || raw === "validating") {
    return "validating";
  }
  if (lower(environmentStatus && environmentStatus.status) === "failed" || raw === "failed" || lower(diagnostics && diagnostics.status) === "failed") {
    return "failed";
  }
  if (lifecycle === "promoted" && !(environmentStatus && environmentStatus.route_active)) {
    return "route activating";
  }
  if (lifecycle === "promoted" || raw === "healthy") {
    return "promoted";
  }
  return raw || lifecycle || "preparing";
}

function deployIsLive(environmentStatus, diagnostics) {
  const lifecycle = lower(environmentStatus && environmentStatus.lifecycle_state);
  const routeActive = Boolean(environmentStatus && environmentStatus.route_active);
  const healthy = lower(environmentStatus && environmentStatus.status) === "healthy";
  const fallbackKnown = Boolean(
    diagnostics
    && diagnostics.route
    && diagnostics.route.route_active === false
    && lower(diagnostics.route.mismatch_reason).includes("application route is not active"),
  );
  return lifecycle === "promoted" && routeActive && healthy && !fallbackKnown;
}

function deployNextAction(stage, live) {
  if (live) {
    return "Live";
  }
  if (stage === "queued") {
    return "Waiting for the existing deployment queue.";
  }
  if (stage === "preparing" || stage === "building") {
    return "Forge is preparing the candidate generation.";
  }
  if (stage === "warming") {
    return "Not live yet. Warmup and validation must finish before promotion.";
  }
  if (stage === "validating") {
    return "Not live yet. Validation and route activation are still in progress.";
  }
  if (stage === "route activating") {
    return "Not live yet. Promotion exists, but the route is not active yet.";
  }
  if (stage === "failed") {
    return "Inspect failure reason, diagnostics, and logs before retrying.";
  }
  return "Not live yet.";
}

function renderDeployTracking(tracking) {
  const container = element("deploy-tracking");
  if (!container) {
    return;
  }
  clearChildren(container);
  if (!tracking) {
    hide("deploy-tracking");
    return;
  }

  const live = deployIsLive(tracking.environmentStatus, tracking.diagnostics);
  const stage = deploymentStage(tracking.deploymentStatus, tracking.environmentStatus, tracking.diagnostics);
  const lines = [
    `Deployment: ${text(tracking.deploymentId)}`,
    `Stage: ${stage}`,
    `Live status: ${live ? "Live" : "Not live yet"}`,
    `Next action: ${deployNextAction(stage, live)}`,
    `Route active: ${tracking.environmentStatus && tracking.environmentStatus.route_active ? "true" : "false"}`,
    `Lifecycle: ${text(tracking.environmentStatus && tracking.environmentStatus.lifecycle_state, "unknown")}`,
  ];

  const failure = tracking.diagnostics
    && Array.isArray(tracking.diagnostics.recent_failures)
    && tracking.diagnostics.recent_failures.find((entry) => entry.deployment_id === tracking.deploymentId)
      || (tracking.diagnostics && tracking.diagnostics.recent_failures && tracking.diagnostics.recent_failures[0]);
  if (failure) {
    lines.push(`Failure stage: ${text(failure.failure_stage)}`);
    lines.push(`Failure reason: ${text(failure.failure_reason)}`);
  }
  if (tracking.diagnostics && tracking.diagnostics.route && tracking.diagnostics.route.mismatch_reason) {
    lines.push(`Route detail: ${tracking.diagnostics.route.mismatch_reason}`);
  }
  (tracking.logs && tracking.logs.validation_failure_summary ? [`Validation summary: ${tracking.logs.validation_failure_summary}`] : []).forEach((line) => lines.push(line));

  lines.forEach((line) => {
    const item = document.createElement("p");
    item.className = "env-preview-summary";
    item.textContent = line;
    container.appendChild(item);
  });

  if (stage === "failed") {
    [
      `forge logs ${tracking.deploymentId}`,
      `forge diagnose ${tracking.projectId} ${tracking.environment}`,
      `forge agent verify-deploy ${tracking.deploymentId}`,
    ].forEach((command) => {
      const item = document.createElement("p");
      item.className = "env-preview-summary";
      item.textContent = command;
      container.appendChild(item);
    });
  }

  show("deploy-tracking");
  setChip("deploy-center-chip", live ? "Live" : stage === "failed" ? "Failed" : "Tracking", live ? "ok" : stage === "failed" ? "warn" : "");
  showState("deploy-center-state", live ? "Deployment promoted and serving the expected route." : deployNextAction(stage, live), live ? "ok" : stage === "failed" ? "warn" : "");
}

async function pollDeploymentTracking(projectId, environment, deploymentId) {
  clearDeployTrackingTimer();
  try {
    const [deploymentStatus, environmentStatus, diagnostics, logs] = await Promise.all([
      fetchApiData(API_PATHS.deploymentStatus(deploymentId)),
      fetchApiData(API_PATHS.environmentStatus(projectId, environment)),
      fetchApiData(API_PATHS.environmentDiagnostics(projectId, environment)),
      fetchApiData(API_PATHS.deploymentLogs(deploymentId)),
    ]);
    dataState.deployTracking = {
      deploymentId,
      projectId,
      environment,
      deploymentStatus,
      environmentStatus,
      diagnostics,
      logs,
    };
    renderDeployTracking(dataState.deployTracking);
    const stage = deploymentStage(deploymentStatus, environmentStatus, diagnostics);
    const live = deployIsLive(environmentStatus, diagnostics);
    if (!live && stage !== "failed") {
      dataState.deployTrackingTimer = window.setTimeout(() => {
        void pollDeploymentTracking(projectId, environment, deploymentId);
      }, 3000);
    } else {
      dataState.deployInFlight = false;
    }
  } catch (error) {
    setChip("deploy-center-chip", "Tracking warn", "warn");
    showState("deploy-center-state", error.message || "Deployment tracking is unavailable right now.", "warn");
  }
}

async function runDeployPreflight() {
  const project = selectedProject();
  if (!project) {
    resetDeployCenterState("Select a project first.", "warn");
    return;
  }
  const environment = selectedDeployEnvironment();
  const gitRef = deployRefValue();
  if (!gitRef) {
    resetDeployCenterState("Enter a Git ref before preflight.", "warn");
    return;
  }

  const button = element("deploy-preview-button");
  if (button) {
    button.disabled = true;
    button.textContent = "Preflighting...";
  }
  showState("deploy-center-state", "Running deploy preflight...", "");

  try {
    const preview = await fetchApiData(API_PATHS.projectDeployPreview(project.project_id), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ environment, ref: gitRef }),
    });
    dataState.deployPreview = preview;
    dataState.deployPreviewValid = Boolean(preview && preview.valid);
    dataState.deployPreviewSignature = deployPreviewSignature(project.project_id, environment, gitRef);
    renderDeployPreview(preview);
    const confirmButton = element("deploy-confirm-button");
    if (confirmButton) {
      confirmButton.disabled = !dataState.deployPreviewValid;
    }
    setChip("deploy-center-chip", dataState.deployPreviewValid ? "Ready" : "Blocked", dataState.deployPreviewValid ? "ok" : "warn");
    showState(
      "deploy-center-state",
      dataState.deployPreviewValid ? "Preflight valid. Review the plan, then confirm deploy." : "Preflight failed. Fix validation errors before deploy.",
      dataState.deployPreviewValid ? "ok" : "warn",
    );
  } catch (error) {
    resetDeployCenterState(error.message || "Deploy preflight failed.", "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "Run Preflight";
    }
  }
}

async function confirmDeploy() {
  const project = selectedProject();
  if (!project) {
    resetDeployCenterState("Select a project first.", "warn");
    return;
  }
  const environment = selectedDeployEnvironment();
  const gitRef = deployRefValue();
  const signature = deployPreviewSignature(project.project_id, environment, gitRef);
  if (!dataState.deployPreviewValid || dataState.deployPreviewSignature !== signature) {
    resetDeployCenterState("No deploy button enabled before valid preflight. Run preflight again.", "warn");
    return;
  }

  const button = element("deploy-confirm-button");
  if (button) {
    button.disabled = true;
    button.textContent = "Queueing...";
  }
  dataState.deployInFlight = true;
  showState("deploy-center-state", "Queueing deployment through the existing deployment FSM...", "");

  try {
    const response = await fetchApiData(API_PATHS.projectDeploy(project.project_id), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ environment, ref: gitRef }),
    });
    dataState.deployTracking = {
      deploymentId: response.deployment_id,
      projectId: project.project_id,
      environment,
      deploymentStatus: { state: "queued" },
      environmentStatus: null,
      diagnostics: null,
      logs: null,
    };
    renderDeployTracking(dataState.deployTracking);
    await pollDeploymentTracking(project.project_id, environment, response.deployment_id);
  } catch (error) {
    dataState.deployInFlight = false;
    resetDeployCenterState(error.message || "Deploy confirmation failed.", "warn");
  } finally {
    if (button) {
      button.textContent = "Confirm Deploy";
      button.disabled = !dataState.deployPreviewValid;
    }
  }
}

function previewTextArea(environment) {
  return element(`env-preview-${environment}`);
}

function previewInputValue(environment) {
  const field = previewTextArea(environment);
  return field ? field.value : "";
}

function resetEnvPreview(message, clearFields, phase = "no_preview") {
  dataState.lastEnvPreview = null;
  dataState.lastEnvPreviewSignature = "";
  dataState.lastEnvApplyIdempotencyKey = "";
  dataState.envApplyInFlight = false;
  setPreviewPhase(phase);
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
  const signature = serializeEnvChanges(currentEnvChanges());
  return Boolean(
    preview
      && !preview.applied
      && !dataState.envApplyInFlight
      && dataState.lastEnvPreviewSignature
      && dataState.lastEnvPreviewSignature === signature
      && environments.length
      && environments.every((environment) => environment && environment.valid)
  );
}

function serializeEnvChanges(changes) {
  return JSON.stringify(changes || {});
}

function generateIdempotencyKey() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID();
  }
  return `env-apply-${Date.now()}-${Math.random().toString(16).slice(2)}`;
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
  const actionCopy = {
    added: "will be added on next deployment",
    updated: "will change on next deployment",
    deleted: "will be removed on next deployment",
    unchanged: "unchanged",
  };
  entries.forEach((entry) => {
    const item = document.createElement("li");
    const beforeValue = entry.before_masked === "<empty>" ? "set to empty string" : text(entry.before_masked, "not configured");
    const afterValue = entry.action === "deleted"
      ? "will be removed on next deployment"
      : entry.after_masked === "<empty>"
        ? "set to empty string"
        : text(entry.after_masked, "not configured");
    item.textContent = `${text(entry.key)} • ${beforeValue} -> ${afterValue} • ${actionCopy[entry.action] || text(entry.action)}`;
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
  summary.appendChild(summaryMetric("Unchanged", environment.unchanged.length));
  summary.appendChild(summaryMetric("Errors", environment.errors.length));
  summary.appendChild(summaryMetric(
    "Revision",
    text(environment.revision_label || `Revision ${environment.base_revision ?? environment.env_store_revision_after ?? 0}`),
  ));
  card.appendChild(summary);

  const validity = document.createElement("p");
  validity.className = `env-preview-validity${environment.valid ? " ok" : " warn"}`;
  validity.textContent = environment.valid ? "Valid preview." : "Preview invalid. Fix errors before retrying.";
  card.appendChild(validity);

  const meta = document.createElement("p");
  meta.className = "env-audit-meta";
  meta.textContent = `Preview is based on revision ${text(environment.base_revision, 0)}. Source ${text(environment.source_label)}. Last updated ${formatUnix(environment.updated_at_unix)}. Updated by ${text(environment.updated_by, "Unknown")}.`;
  card.appendChild(meta);

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

  const operatorCopy = document.createElement("p");
  operatorCopy.textContent = "Applies on next deployment. Current running generation is unchanged. Rollback uses sealed historical snapshots.";
  note.appendChild(operatorCopy);

  if (preview && preview.warning) {
    const warning = document.createElement("p");
    warning.textContent = preview.warning;
    note.appendChild(warning);
  }

  result.appendChild(note);

  const environments = preview && Array.isArray(preview.environments) ? preview.environments : [];
  const isApplyResult = preview
    && typeof preview.status === "string"
    && (preview.status === "applied" || preview.status === "idempotent_replay");
  const allValid = environments.length && environments.every((environment) => environment && environment.valid);
  if (preview && preview.status === "idempotent_replay") {
    setPreviewPhase("idempotent_replay");
  } else if (isApplyResult) {
    setPreviewPhase("applied");
  } else if (allValid) {
    setPreviewPhase("preview_valid");
  } else {
    setPreviewPhase("preview_has_errors");
  }
  environments.forEach((environment) => {
    result.appendChild(previewSummaryCard(environment));
  });

  dataState.lastEnvPreview = preview || null;
  if (preview && !isApplyResult && !preview.applied) {
    dataState.lastEnvPreviewSignature = serializeEnvChanges(currentEnvChanges());
    dataState.lastEnvApplyIdempotencyKey = generateIdempotencyKey();
  } else {
    dataState.lastEnvPreviewSignature = "";
    dataState.lastEnvApplyIdempotencyKey = "";
  }
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
  setPreviewPhase("no_preview");
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
    dataState.lastEnvPreview = null;
    dataState.lastEnvPreviewSignature = "";
    dataState.lastEnvApplyIdempotencyKey = "";
    setPreviewPhase("preview_has_errors");
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
  const entries = Array.isArray(diff) ? diff : [];
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.className = "env-audit-meta";
    empty.textContent = "No effective changes.";
    wrapper.appendChild(empty);
    return wrapper;
  }
  appendPreviewEntries(wrapper, "Masked diff", entries);
  return wrapper;
}

function filteredAuditEntries(audit) {
  const entries = audit && Array.isArray(audit.entries) ? audit.entries.slice() : [];
  return entries
    .sort((left, right) => (right.modified_at_unix || 0) - (left.modified_at_unix || 0))
    .filter((entry) => {
      const envMatch = uiState.envAuditEnvironment === "all" || entry.environment === uiState.envAuditEnvironment;
      const statusMatch = uiState.envAuditStatus === "all" || entry.status === uiState.envAuditStatus;
      return envMatch && statusMatch;
    });
}

function renderEnvAuditHistory(audit) {
  const list = element("env-audit-history");
  if (!list) {
    return;
  }
  clearChildren(list);

  const allEntries = audit && Array.isArray(audit.entries) ? audit.entries : [];
  const total = audit && typeof audit.total === "number" ? audit.total : allEntries.length;
  const filtered = filteredAuditEntries(audit);
  setChip("env-audit-total-chip", `${filtered.length}/${total} events`, total ? "" : "stale");

  if (!allEntries.length) {
    hide("env-audit-history");
    showState("env-audit-state", "No env changes recorded yet.");
    return;
  }

  if (!filtered.length) {
    hide("env-audit-history");
    showState("env-audit-state", "No env changes match the current filters.");
    return;
  }

  filtered.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "env-audit-card";

    const head = document.createElement("div");
    head.className = "env-audit-head";
    const title = document.createElement("h3");
    title.textContent = `${text(entry.environment)} • ${text(entry.audit_status_label || entry.status)}`;
    const meta = document.createElement("p");
    meta.className = "env-audit-meta";
    meta.textContent = `Requested by ${text(entry.requested_by, "Unknown")} • ${formatUnix(entry.modified_at_unix)} • ${text(entry.source_label, "Latest configured env store")}`;
    head.append(title, meta);
    card.appendChild(head);

    const summary = document.createElement("div");
    summary.className = "env-preview-summary";
    summary.appendChild(summaryMetric("Added", entry.summary && entry.summary.added ? entry.summary.added : 0));
    summary.appendChild(summaryMetric("Updated", entry.summary && entry.summary.updated ? entry.summary.updated : 0));
    summary.appendChild(summaryMetric("Deleted", entry.summary && entry.summary.deleted ? entry.summary.deleted : 0));
    if (entry.summary && entry.summary.unchanged) {
      summary.appendChild(summaryMetric("Unchanged", entry.summary.unchanged));
    }
    summary.appendChild(summaryMetric(
      "Revision",
      text(entry.revision_label || `${text(entry.env_store_revision_before, 0)} -> ${text(entry.env_store_revision_after, 0)}`),
    ));
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

  const preview = dataState.lastEnvPreview;
  const environments = preview && Array.isArray(preview.environments) ? preview.environments : [];
  const expectedBaseRevisions = { development: 0, staging: 0, production: 0 };
  const previewHashes = { development: "", staging: "", production: "" };
  environments.forEach((environment) => {
    if (!environment || !environment.environment) {
      return;
    }
    expectedBaseRevisions[environment.environment] = environment.base_revision || 0;
    previewHashes[environment.environment] = environment.preview_hash || "";
  });

  const button = element("env-apply-confirm-button");
  const openButton = element("env-apply-button");
  dataState.envApplyInFlight = true;
  if (button) {
    button.disabled = true;
    button.textContent = "Applying...";
  }
  if (openButton) {
    openButton.disabled = true;
  }
  setPreviewPhase("applying");
  showState("env-preview-state", "Applying masked environment changes...");

  try {
    const response = await fetchApiData(API_PATHS.projectEnvApply(project.project_id), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        changes: currentEnvChanges(),
        expected_base_revisions: expectedBaseRevisions,
        preview_hashes: previewHashes,
        idempotency_key: dataState.lastEnvApplyIdempotencyKey,
      }),
    });
    dataState.envInventoryByProject.delete(project.project_id);
    dataState.envAuditByProject.delete(project.project_id);
    hide("env-apply-confirmation");
    renderEnvPreviewResult(response);
    showState("env-preview-state", response.message || "Applied. These changes will affect the next deployment.");
    const [inventory, audit] = await Promise.all([
      ensureProjectEnvInventory(project.project_id),
      ensureProjectEnvAudit(project.project_id),
    ]);
    renderEnvInventory(inventory);
    renderEnvAuditHistory(audit);
  } catch (error) {
    const message = error.status === 409
      ? "Environment changed since preview. Refresh preview and try again. No changes were saved."
      : (error.message || "Apply failed.");
    setPreviewPhase(error.status === 409 ? "preview_stale" : "apply_failed");
    showState("env-preview-state", message, "warn");
    if (error.status === 409) {
      dataState.lastEnvPreview = null;
      dataState.lastEnvPreviewSignature = "";
      dataState.lastEnvApplyIdempotencyKey = "";
      hide("env-apply-panel");
      hide("env-apply-confirmation");
    }
  } finally {
    dataState.envApplyInFlight = false;
    if (button) {
      button.disabled = false;
      button.textContent = "Confirm Apply";
    }
    if (openButton) {
      openButton.disabled = false;
    }
  }
}

function invalidateEnvPreviewIfDirty() {
  if (!dataState.lastEnvPreviewSignature) {
    return;
  }
  const signature = serializeEnvChanges(currentEnvChanges());
  if (signature === dataState.lastEnvPreviewSignature && dataState.lastEnvPreview && !dataState.lastEnvPreview.applied) {
    return;
  }
  resetEnvPreview("Edit detected. Preview again before applying.", false, "no_preview");
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
  const githubSearch = element("github-repo-search");
  if (githubSearch) {
    githubSearch.addEventListener("input", (event) => {
      uiState.githubRepoQuery = event.target.value || "";
      renderGitHubRepoList();
    });
  }

  const githubRefresh = element("github-repo-refresh");
  if (githubRefresh) {
    githubRefresh.addEventListener("click", () => {
      void loadGitHubRepositories();
    });
  }

  const githubRegisterButton = element("github-register-button");
  if (githubRegisterButton) {
    githubRegisterButton.addEventListener("click", () => {
      void registerProjectFromGitHub();
    });
  }

  const githubPreviewButton = element("github-preview-refresh");
  if (githubPreviewButton) {
    githubPreviewButton.addEventListener("click", () => {
      void refreshGitHubRegistrationPreview();
    });
  }

  const githubProjectIdInput = element("github-project-id-input");
  if (githubProjectIdInput) {
    githubProjectIdInput.addEventListener("input", () => {
      resetGitHubProjectConfirmation();
      updateGitHubRegisterButton(dataState.githubRegistrationPreview);
    });
  }

  const githubBaseDomainInput = element("github-base-domain-input");
  if (githubBaseDomainInput) {
    githubBaseDomainInput.addEventListener("input", () => {
      resetGitHubProjectConfirmation();
      updateGitHubRegisterButton(dataState.githubRegistrationPreview);
    });
  }

  const githubProjectIdConfirm = element("github-project-id-confirm");
  if (githubProjectIdConfirm) {
    githubProjectIdConfirm.addEventListener("change", () => {
      updateGitHubRegisterButton(dataState.githubRegistrationPreview);
    });
  }

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
      dataState.deploymentHistoryByEnvironment.clear();
      dataState.generationTruthByEnvironment.clear();
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

  const auditEnvironment = element("env-audit-environment-filter");
  if (auditEnvironment) {
    auditEnvironment.addEventListener("change", (event) => {
      uiState.envAuditEnvironment = event.target.value || "all";
      renderEnvAuditHistory(dataState.envAuditByProject.get(uiState.selectedProjectId) || null);
    });
  }

  const auditStatus = element("env-audit-status-filter");
  if (auditStatus) {
    auditStatus.addEventListener("change", (event) => {
      uiState.envAuditStatus = event.target.value || "all";
      renderEnvAuditHistory(dataState.envAuditByProject.get(uiState.selectedProjectId) || null);
    });
  }

  const previewButton = element("env-preview-button");
  if (previewButton) {
    previewButton.addEventListener("click", () => {
      void submitEnvPreview();
    });
  }

  const deployPreviewButton = element("deploy-preview-button");
  if (deployPreviewButton) {
    deployPreviewButton.addEventListener("click", () => {
      void runDeployPreflight();
    });
  }

  const deployConfirmButton = element("deploy-confirm-button");
  if (deployConfirmButton) {
    deployConfirmButton.addEventListener("click", () => {
      void confirmDeploy();
    });
  }

  const deployEnvironment = element("deploy-environment-select");
  if (deployEnvironment) {
    deployEnvironment.addEventListener("change", () => {
      syncDeployCenterForSelectedProject();
    });
  }

  const truthEnvironment = element("history-environment-select");
  if (truthEnvironment) {
    truthEnvironment.addEventListener("change", () => {
      const project = selectedProject();
      if (project) {
        void renderDeploymentTruth(project.project_id, []);
      }
    });
  }

  const deployRef = element("deploy-ref-input");
  if (deployRef) {
    deployRef.addEventListener("input", () => {
      syncDeployCenterForSelectedProject();
    });
  }

  ["development", "staging", "production"].forEach((environment) => {
    const field = previewTextArea(environment);
    if (!field) {
      return;
    }
    field.addEventListener("input", invalidateEnvPreviewIfDirty);
  });

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
setPreviewPhase("no_preview");
updateGitHubRegisterButton(null);
resetDeployCenterState("Select a project, choose an environment, enter a Git ref, then run preflight.");
void loadConsole();
