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
  /** Quelques valeurs par colonne, pour que le conseiller voie les données. */
  sample_data?: Record<string, any[]>;
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

// ── Passe sous contrat ─────────────────────────────────────────────────
// Le cœur du produit : un usage déclaré -> un contrat -> une réparation
// dans ce sens -> une vérification qui ne consulte pas le nettoyeur.

export type UsageKey = 'base' | 'modele' | 'tableau' | 'jointure';

export interface ContratColonne {
  nom: string;
  type: string;
  nullable: boolean;
  taux_nul_max: number | null;
  unique: boolean;
  domaine: string[] | null;
  minimum: number | null;
  maximum: number | null;
  forme: string | null;
  casse_significative?: boolean;
  politique_aberrantes: string;
}

export interface Contrat {
  colonnes: ContratColonne[];
  cle_primaire: string[] | null;
  sans_doublons: boolean;
  lignes_perdues_max: number;
  imputation_max: number;
  usage: string;
}

export interface Rapport {
  tient: boolean;
  score: number;
  lignes: number;
  colonnes: number;
  violations: any[];
  satisfait: string[];
}

export interface Proposition {
  usage: UsageKey;
  libelle: string;
  contrat: Contrat;
  contrat_lisible: string;
  arbitrages: string[];
  etat_initial: Rapport;
  dataset: { filename: string; rows: number; columns: number; size_bytes: number };
}

export interface JournalEntree { action: string; colonne: string | null; detail: string; mesure: number | null }

export interface PassageContrat {
  usage: UsageKey;
  contrat: Contrat;
  arbitrages: string[];
  score_avant: number;
  score_apres: number;
  contrat_tenu: boolean;
  violations_avant: number;
  violations_apres: number;
  rapport: Rapport;
  journal: { faits: JournalEntree[]; refuses: JournalEntree[] };
  enonce: { garanti: string[]; non_garanti: string[]; fait: string[]; [k: string]: string[] };
  lignes: [number, number];
  colonnes: [number, number];
  csv_base64: string;
  parquet_base64: string;
  script: string;
  message: string;
}

export interface PlanPreparation {
  plan: {
    cible: string | null;
    lignes: number;
    numeriques: string[];
    categorielles: string[];
    ecartees: { nom: string; raison: string; modalites: number | null }[];
    refus: string[];
    modalites_max: number;
    utilisable: boolean;
  };
  script: string;
  filename: string;
  message: string;
}

const formulaire = (file: File, champs: Record<string, string | undefined>) => {
  const fd = new FormData();
  fd.append('file', file);
  for (const [k, v] of Object.entries(champs)) if (v !== undefined) fd.append(k, v);
  return fd;
};

export const proposerContrat = async (file: File, usage: UsageKey): Promise<Proposition> =>
  (await apiFormData.post('/contract-clean/propose', formulaire(file, { usage }))).data;

export const passerContrat = async (file: File, usage: UsageKey, contrat?: Contrat): Promise<PassageContrat> =>
  (await apiFormData.post('/contract-clean/',
    formulaire(file, { usage, contrat: contrat ? JSON.stringify(contrat) : undefined }))).data;

export const preparerChaine = async (file: File, cible?: string): Promise<PlanPreparation> =>
  (await apiFormData.post('/prepare/', formulaire(file, { cible: cible || undefined, usage: 'modele' }))).data;

/** Télécharge un contenu base64 ou texte sous un nom donné. */
export const telecharger = (contenu: string, nom: string, type: string, base64 = false) => {
  const data = base64 ? Uint8Array.from(atob(contenu), (c) => c.charCodeAt(0)) : contenu;
  const url = URL.createObjectURL(new Blob([data], { type }));
  const a = document.createElement('a');
  a.href = url;
  a.download = nom;
  a.click();
  URL.revokeObjectURL(url);
};

// ── Diagnostic de jointure ─────────────────────────────────────────────
// Ce que la jointure ferait, sans la faire.

export interface DiagnosticJointure {
  joignable: boolean;
  cle_gauche: string[];
  cle_droite: string[];
  lignes_gauche: number;
  lignes_droite: number;
  cardinalite: '1:1' | 'n:1' | '1:n' | 'n:m';
  couverture: number;
  couverture_min: number;
  orphelines: number;
  exemples_orphelines: string[];
  presque: number;
  exemples_presque: [string, string][];
  cles_vides_gauche: number;
  cles_vides_droite: number;
  cles_droite_inutilisees: number;
  lignes_resultat: number;
  problemes: { code: string; message: string }[];
}

export const colonnesDuFichier = async (file: File): Promise<{ colonnes: string[]; lignes: number }> =>
  (await apiFormData.post('/join/colonnes', formulaire(file, {}))).data;

export const diagnostiquerJointure = async (
  gauche: File, droite: File, paires: [string, string][], couvertureMin: number,
): Promise<DiagnosticJointure> => {
  const fd = new FormData();
  fd.append('gauche', gauche);
  fd.append('droite', droite);
  fd.append('sur', paires.map(([a, b]) => `${a}=${b}`).join(','));
  fd.append('couverture_min', String(couvertureMin));
  return (await apiFormData.post('/join/', fd)).data;
};
