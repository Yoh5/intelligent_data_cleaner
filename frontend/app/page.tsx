'use client';

import { useEffect, useState } from 'react';
import { FileUpload } from '@/components/FileUpload';
import { AnalysisResult } from '@/components/AnalysisResult';
import { CodeExport } from '@/components/CodeExport';
import { checkBackendHealth } from '@/lib/api';

type AppState = 'idle' | 'analysis' | 'autopilot';

interface AutopilotResult {
  script: string | null;
  filename: string | null;
  issues_found: number;
  steps_applied: number;
  message: string;
  analysis: { rows: number; columns: number; issues: any[] };
}

export default function Home() {
  const [appState, setAppState] = useState<AppState>('idle');
  const [analysis, setAnalysis] = useState<any>(null);
  const [autopilotResult, setAutopilotResult] = useState<AutopilotResult | null>(null);
  const [originalFile, setOriginalFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [backendOffline, setBackendOffline] = useState(false);

  // Health check on mount
  useEffect(() => {
    checkBackendHealth().then((ok) => {
      if (!ok) setBackendOffline(true);
    });
  }, []);

  const handleAnalysisComplete = (result: any) => {
    setAnalysis(result);
    setAppState('analysis');
    setError(null);
  };

  const handleAutopilotComplete = (result: AutopilotResult) => {
    setAutopilotResult(result);
    setAppState('autopilot');
    setError(null);
  };

  const reset = () => {
    setAppState('idle');
    setAnalysis(null);
    setAutopilotResult(null);
    setOriginalFile(null);
    setError(null);
  };

  return (
    <main className="min-h-screen">
      {/* Backend offline warning */}
      {backendOffline && (
        <div className="wrap">
          <p className="alert alert--bad" style={{ marginTop: '1rem' }}>
            Le serveur d&apos;analyse ne répond pas sur le port 8000. Lancez-le, puis rechargez.
            <button onClick={() => setBackendOffline(false)} className="btn btn--quiet ml-4">
              masquer
            </button>
          </p>
        </div>
      )}

      <div className="wrap">
        <div className="topbar">
          <span className="topbar-name">Intelligent Data Cleaner</span>
          <span className="meta">{backendOffline ? 'serveur hors ligne' : 'prêt'}</span>
        </div>

        <div style={{ padding: '2.6rem 0 2.4rem' }}>
          <h1 className="title">Ce qui ne va pas dans vos données, colonne par colonne.</h1>
          <p className="lede" style={{ marginTop: '0.6rem' }}>
            Déposez un CSV ou un Excel. Vous décidez quoi faire de chaque défaut, et vous
            repartez avec le fichier nettoyé et le script Python qui l&apos;a nettoyé —
            lisible, et rejouable sans cette application.
          </p>
        </div>

        {appState === 'idle' && (
          <div style={{ maxWidth: '34rem' }}>
            <FileUpload
              onAnalysisComplete={handleAnalysisComplete}
              onAutopilotComplete={handleAutopilotComplete}
              onFileReady={setOriginalFile}
              onError={setError}
            />
            {error && (
              <div className="mt-4">
                <p className="alert alert--bad">{error}</p>
                <button
                  onClick={() => setError(null)}
                  className="btn btn--quiet mt-2"
                >
                  Réessayer
                </button>
              </div>
            )}
          </div>
        )}

        {appState === 'analysis' && analysis && (
          <div>
            <button onClick={reset} className="btn btn--quiet mb-6">
              analyser un autre fichier
            </button>
            <AnalysisResult
              analysis={{
                ...analysis,
                shape: [analysis.dataset_info?.rows || 0, analysis.dataset_info?.columns || 0],
                issues: analysis.issues || [],
              }}
              originalData={[]}
              originalFile={originalFile}
            />
          </div>
        )}

        {appState === 'autopilot' && autopilotResult && (
          <div>
            <button onClick={reset} className="btn btn--quiet mb-6">
              analyser un autre fichier
            </button>

            <div className="panel">
              <p className="meta">
                {autopilotResult.analysis.rows.toLocaleString('fr-FR')} lignes
                <span className="meta-sep">·</span>
                {autopilotResult.issues_found} défauts trouvés
                <span className="meta-sep">·</span>
                {autopilotResult.steps_applied} corrigés
              </p>
              <p className="lede" style={{ marginTop: '0.5rem' }}>{autopilotResult.message}</p>
            </div>

            {autopilotResult.script ? (
              <CodeExport
                steps={[]}
                generatedScript={autopilotResult.script}
                filename={autopilotResult.filename || 'auto_clean.py'}
                originalFile={originalFile}
              />
            ) : (
              <p className="alert">
                Rien à corriger : le fichier peut être utilisé tel quel.
              </p>
            )}
          </div>
        )}
      </div>
    </main>
  );
}
