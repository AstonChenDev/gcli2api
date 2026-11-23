import { getBasePath } from "./basePath.js";

// Deployments under the same origin can have different panel passwords.
export const panelTokenKey = `agy2apiPanelToken:${getBasePath()}`;

export function getPanelToken() {
  return localStorage.getItem(panelTokenKey);
}

export function expirePanelSession(authorization) {
  const current = getPanelToken();
  if (current && authorization === `Bearer ${current}`) {
    localStorage.removeItem(panelTokenKey);
    window.location.href = getBasePath();
  }
}
