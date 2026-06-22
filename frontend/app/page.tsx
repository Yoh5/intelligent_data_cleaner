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
    <main className="min-h-screen bg-gray-50">
      {/* Backend offline warning */}
      {backendOffline && (
        <div className="bg-red-600 text-white text-sm text-center py-2 px-4">
          Backend inaccessible — assurez-vous que le serveur tourne sur le port 8000.
          <button onClick={() => setBackendOffline(false)} className="ml-4 underline hover:no-underline">
            Ignorer
          </button>
        </div>
      )}

      <div className="max-w-6xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-extrabold text-gray-900 sm:text-5xl mb-4">
            Intelligent Data Cleaner
          </h1>
          <p className="text-xl text-gray-600 max-w-2xl mx-auto">
            Analysez et nettoyez vos données automatiquement. Détection des problèmes,
            suggestions de stratégies et génération de scripts Python prêts à l'emploi.
          </p>
        </div>

        {appState === 'idle' && (
          <div className="bg-white shadow-xl rounded-2xl p-8 max-w-2xl mx-auto">
            <FileUpload
              onAnalysisComplete={handleAnalysisComplete}
              onAutopilotComplete={handleAutopilotComplete}
              onFileReady={setOriginalFile}
              onError={setError}
            />
            {error && (
              <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
                <p className="text-red-800 text-center text-sm">{error}</p>
                <button
                  onClick={() => setError(null)}
                  className="mt-2 block mx-auto text-xs text-red-600 hover:underline"
                >
                  Réessayer
                </button>
              </div>
            )}
          </div>
        )}

        {appState === 'analysis' && analysis && (
          <div>
            <button onClick={reset} className="mb-6 text-blue-600 hover:text-blue-800 font-medium text-sm">
              ← Analyser un autre fichier
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
            <button onClick={reset} className="mb-6 text-blue-600 hover:text-blue-800 font-medium text-sm">
              ← Analyser un autre fichier
            </button>

            {/* Autopilot summary banner */}
            <div className="bg-gradient-to-r from-purple-50 to-indigo-50 border border-purple-200 rounded-xl p-6 mb-6">
              <div className="flex items-start gap-4">
                <div className="text-3xl">⚡</div>
                <div>
                  <h2 className="text-lg font-bold text-purple-900 mb-1">Nettoyage automatique terminé</h2>
                  <p className="text-purple-700 text-sm">{autopilotResult.message}</p>
                  <div className="flex gap-6 mt-3">
                    <div className="text-center">
                      <div className="text-2xl font-bold text-purple-800">
                        {autopilotResult.analysis.rows.toLocaleString()}
                      </div>
                      <div className="text-xs text-gray-500">Lignes</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-orange-600">{autopilotResult.issues_found}</div>
                      <div className="text-xs text-gray-500">Problèmes détectés</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-600">{autopilotResult.steps_applied}</div>
                      <div className="text-xs text-gray-500">Corrections appliquées</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {autopilotResult.script ? (
              <CodeExport
                steps={[]}
                generatedScript={autopilotResult.script}
                filename={autopilotResult.filename || 'auto_clean.py'}
                originalFile={originalFile}
              />
            ) : (
              <div className="bg-green-50 border border-green-200 rounded-xl p-8 text-center">
                <p className="text-green-800 font-medium">
                  Aucun problème détecté — votre dataset est déjà propre !
                </p>
              </div>
            )}
          </div>
        )}
      </div>
    </main>
  );
}
