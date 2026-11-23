import axios from "axios";
import { getBasePath } from "./basePath.js";
import { expirePanelSession, getPanelToken } from "./panelSession.js";

const api = axios.create({
  // 基径取面板页面所在路径（末尾补斜杠），反代到子路径下时请求仍落在同一子路径内
  baseURL: getBasePath(),
  timeout: 120000,
});

// 请求拦截器 - 添加token
api.interceptors.request.use((config) => {
  const token = getPanelToken();
  if (token && config.url !== "/auth/login") {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// 响应拦截器 - 处理错误
api.interceptors.response.use(
  (response) => response,
  (error) => {
    const credentialFailure = /^\/creds\/(test|quota)\//.test(error.config?.url) && error.response?.data?.email;
    if (error.response?.status === 401 && error.config?.url !== "/auth/login" && !credentialFailure) {
      expirePanelSession(error.config?.headers?.Authorization);
    }
    return Promise.reject(error);
  }
);

export default api;
