'use client';

import { useEffect, useState } from 'react';
import { FileUpload } from '@/components/FileUpload';
import { AnalysisResult } from '@/components/AnalysisResult';
import { CodeExport } from '@/components/CodeExport';
import { checkBackendHealth } from '@/lib/api';
import { Gate } from '@/components/Gate';
import { Jointure } from '@/components/Jointure';
import { HeroArt, IconDatabase } from '@/components/Illustrations';

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
  const [onglet, setOnglet] = useState<'contrat' | 'jointure' | 'colonnes'>('contrat');
  const detaille = onglet === 'colonnes';

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
        <header className="topbar">
          <span className="brand">
            <IconDatabase size={20} />
            <span className="topbar-name">Intelligent Data Cleaner</span>
          </span>
          <span className="meta">
            <span className={`status-dot ${backendOffline ? 'status-dot--off' : ''}`} aria-hidden="true" />
            {backendOffline ? 'serveur hors ligne' : 'serveur prêt'}
          </span>
        </header>

        <section className="hero">
          <div>
            <p className="eyebrow">Contrôle avant chargement</p>
            <h1 className="title" style={{ marginTop: '0.7rem' }}>
              Ce fichier peut-il entrer en base&nbsp;? Et qu&apos;a-t-on refusé d&apos;inventer&nbsp;?
            </h1>
            <p className="lede" style={{ marginTop: '1rem' }}>
              Déclarez ce que vous allez faire des données. L&apos;outil en déduit un contrat, répare
              dans ce sens, puis vérifie le résultat sans consulter le nettoyeur. Le rapport dit ce
              qui est garanti, ce qui ne l&apos;est pas, et ce qui a été refusé exprès.
            </p>
          </div>
          <div className="hero-art"><HeroArt /></div>
        </section>

        <div className="flex items-center gap-4 flex-wrap" style={{ marginBottom: '1.5rem' }}>
          {([
            ['contrat', 'Contrôle sous contrat'],
            ['jointure', 'Jointure de deux fichiers'],
            ['colonnes', 'Nettoyage colonne par colonne'],
          ] as const).map(([cle, nom]) => (
            <button key={cle} className={`btn ${onglet === cle ? 'btn--primary' : ''}`}
                    onClick={() => setOnglet(cle)} aria-pressed={onglet === cle}>
              {nom}
            </button>
          ))}
          <span className="meta">
            {onglet === 'contrat' ? 'un fichier peut-il entrer ? le rapport vérifié indépendamment'
              : onglet === 'jointure' ? 'ce que la jointure ferait, avant de la faire'
              : 'vous choisissez chaque stratégie, et repartez avec le script'}
          </span>
        </div>

        {onglet === 'contrat' && <Gate />}
        {onglet === 'jointure' && <Jointure />}

        {detaille && appState === 'idle' && (
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

        {detaille && appState === 'analysis' && analysis && (
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

        {detaille && appState === 'autopilot' && autopilotResult && (
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
        <footer className="foot">
          Le contrat est vérifié par un module qui ne lit que les données, jamais l&apos;avis du nettoyeur.
          Code source et mesures :{' '}
          <a href="https://github.com/Yoh5/intelligent_data_cleaner">github.com/Yoh5/intelligent_data_cleaner</a>
        </footer>
      </div>
    </main>
  );
}
