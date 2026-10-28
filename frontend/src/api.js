import axios from "axios";

const api = axios.create({
  baseURL: window.location.pathname.startsWith("/preview/") ? "/preview" : "",
  timeout: 120000,
});

// 请求拦截器 - 添加token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("agy2apiPreviewPanelToken");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// 响应拦截器 - 处理错误
api.interceptors.response.use(
  (response) => response,
  (error) => {
    const credentialTestFailure = error.config?.url?.startsWith("/creds/test/") && error.response?.data?.email;
    if (error.response?.status === 401 && !credentialTestFailure) {
      localStorage.removeItem("agy2apiPreviewPanelToken");
      window.location.href = window.location.pathname.startsWith("/preview/") ? "/preview/" : "/";
    }
    return Promise.reject(error);
  }
);

export default api;
