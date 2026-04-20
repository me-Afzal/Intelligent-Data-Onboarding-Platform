import axios from "axios";

export const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export const api = axios.create({
  baseURL: API_BASE,
});

// Attach JWT token to every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("codeace_token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// On 401, clear the token and notify the app to show the login page
api.interceptors.response.use(
  (response) => response,
  (error) => {
    const isAuthRoute = error.config?.url?.includes("/api/auth/");
    if (error.response?.status === 401 && !isAuthRoute) {
      localStorage.removeItem("codeace_token");
      window.dispatchEvent(new Event("codeace:unauthorized"));
    }
    return Promise.reject(error);
  }
);

// Include the JWT token as a query param so the WebSocket endpoint can authenticate
export function wsUrl(jobId) {
  const token = localStorage.getItem("codeace_token") || "";
  const base = API_BASE.replace(/^http/, "ws");
  return `${base}/ws/jobs/${jobId}?token=${encodeURIComponent(token)}`;
}
