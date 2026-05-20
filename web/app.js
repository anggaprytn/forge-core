async function updateStatus(path, elementId) {
  const node = document.getElementById(elementId);
  if (!node) {
    return;
  }

  try {
    const response = await fetch(path, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    if (!response.ok) {
      node.textContent = "Unavailable";
      return;
    }

    const data = await response.json();
    node.textContent = typeof data.status === "string" ? data.status : "Unknown";
  } catch (_error) {
    node.textContent = "Unavailable";
  }
}

void updateStatus("/healthz", "healthz-status");
void updateStatus("/readyz", "readyz-status");
