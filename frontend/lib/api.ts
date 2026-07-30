import axios, { AxiosError } from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

const normalizeError = (err: unknown): never => {
  if (err instanceof AxiosError) {
    const detail = err.response?.data?.detail;
    const msg =
      typeof detail === 'string'
        ? detail
        : Array.isArray(detail)
        ? detail.map((d: any) => d.msg || JSON.stringify(d)).join(', ')
        : err.message || 'Erreur inconnue';
    throw new Error(msg);
  }
  throw err;
};

export const api = axios.create({
  baseURL: API_URL,
  headers: { 'Content-Type': 'application/json' },
  timeout: 60000,
});
api.interceptors.response.use((r) => r, normalizeError);

const apiFormData = axios.create({ baseURL: API_URL, timeout: 120000 });
apiFormData.interceptors.response.use((r) => r, normalizeError);

export interface DatasetInfo {
  filename: string;
  rows: number;
  columns: number;
  size_bytes: number;
  column_types: Record<string, string>;
}

export interface IssueDetected {
  type: string;
  severity: 'high' | 'medium' | 'low' | 'critical';
  column: string | null;
  description: string;
  affected_rows: number | null;
  semantic_type?: string;
}

export interface AnalysisResult {
  id: string;
  created_at: string;
  dataset_info: DatasetInfo;
  issues: IssueDetected[];
  profile_html?: string;
  raw_profile?: {
    dtypes?: Record<string, string>;
    total_missing?: number;
    sample_rows?: Array<Record<string, any>>;
  };
}

export const analyzeFile = async (file: File): Promise<AnalysisResult> => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await apiFormData.post('/analyze/', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return response.data;
};

export interface CleaningStrategy {
  name: string;
  description: string;
  pros: string[];
  cons: string[];
  code_preview: string;
  confidence: 'high' | 'medium' | 'low';
}

export interface CleaningStep {
  column: string | null;
  issue_type: string;
  strategy_name: string;
  code: string;
}

export interface CodeGenerationRequest {
  dataset_name: string;
  steps: CleaningStep[];
}

export interface CodeGenerationResponse {
  script: string;
  filename: string;
}

export const generateCode = async (
  request: CodeGenerationRequest
): Promise<CodeGenerationResponse> => {
  const response = await api.post('/generate/', request);
  return response.data;
};

export interface BatchSuggestionRequest {
  dataset_name: string;
  column_types: Record<string, string>;
  issues: IssueDetected[];
  sample_data: Record<string, any>[];
}

export interface BatchStrategyItem {
  issue: IssueDetected;
  strategies: CleaningStrategy[];
  recommended: number;
  rationale?: string;   // justification du conseiller LLM (si activé)
}

export interface BatchSuggestionResponse {
  results: BatchStrategyItem[];
  total_issues: number;
  total_strategies: number;
  advisor_used?: boolean;   // true si le conseiller LLM a pesé
}

export const getSuggestionsBatch = async (
  request: BatchSuggestionRequest
): Promise<BatchSuggestionResponse> => {
  const response = await api.post('/suggest/batch', request);
  return response.data;
};

// ── Autopilot ────────────────────────────────────────────────────────────────

export interface AutoCleanResponse {
  script: string | null;
  filename: string | null;
  issues_found: number;
  steps_applied: number;
  message: string;
  analysis: {
    rows: number;
    columns: number;
    issues: any[];
  };
}

export const autoClean = async (file: File): Promise<AutoCleanResponse> => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await apiFormData.post('/auto-clean/', formData);
  return response.data;
};

// ── Execute ──────────────────────────────────────────────────────────────────

export interface ExecuteStats {
  rows_before: number;
  rows_after: number;
  rows_removed: number;
  null_before: number;
  null_after: number;
  null_fixed: number;
}

export interface ExecuteResponse {
  file_base64: string;
  filename: string;
  mimetype: string;
  stats: ExecuteStats | null;
  logs: string;
}

export const executeScript = async (file: File, script: string): Promise<ExecuteResponse> => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('script', script);
  const response = await apiFormData.post('/execute/', formData);
  return response.data;
};

// ── URL ingestion ─────────────────────────────────────────────────────────────

export const analyzeFromUrl = async (url: string): Promise<AnalysisResult> => {
  const response = await api.post('/analyze/url', { url });
  return response.data;
};

// ── Health check ──────────────────────────────────────────────────────────────

export const checkBackendHealth = async (): Promise<boolean> => {
  try {
    const response = await api.get('/', { timeout: 5000 });
    return response.status === 200;
  } catch {
    return false;
  }
};
