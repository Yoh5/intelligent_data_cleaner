import axios from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_URL,
  headers: { 'Content-Type': 'application/json' },
});

const apiFormData = axios.create({ baseURL: API_URL });

export interface DatasetInfo {
  filename: string;
  rows: number;
  columns: number;
  size_bytes: number;
  column_types: Record<string, string>;
}

export interface IssueDetected {
  type: string;
  severity: 'high' | 'medium' | 'low';
  column: string | null;
  description: string;
  affected_rows: number | null;
}

export interface AnalysisResult {
  id: string;
  created_at: string;
  dataset_info: DatasetInfo;
  issues: IssueDetected[];
  profile_html?: string;
  raw_profile?: Record<string, any>;
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
}

export interface BatchSuggestionResponse {
  results: BatchStrategyItem[];
  total_issues: number;
  total_strategies: number;
}

export const getSuggestionsBatch = async (
  request: BatchSuggestionRequest
): Promise<BatchSuggestionResponse> => {
  const response = await api.post('/suggest/batch', request);
  return response.data;
};
