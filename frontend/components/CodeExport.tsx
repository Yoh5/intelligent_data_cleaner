'use client';

import React, { useState, useEffect } from 'react';
import type { ExecuteStats } from '@/lib/api';

interface CodeStep {
  strategy_name: string;
  column?: string;
  code: string;
}

interface CodeExportProps {
  steps: CodeStep[];
  generatedScript?: string;
  filename?: string;
  originalFile?: File | null;
  onRegenerate?: () => void;
}

export const CodeExport: React.FC<CodeExportProps> = ({
  steps,
  generatedScript,
  filename = 'clean_dataset.py',
  originalFile,
  onRegenerate,
}) => {
  const [copied, setCopied] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executeStats, setExecuteStats] = useState<ExecuteStats | null>(null);
  const [executeError, setExecuteError] = useState<string | null>(null);

  // Reset execution state when the script changes (e.g., after "Recommencer")
  useEffect(() => {
    setExecuteStats(null);
    setExecuteError(null);
  }, [generatedScript]);

  const copyToClipboard = () => {
    if (generatedScript) {
      navigator.clipboard.writeText(generatedScript);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const downloadScript = () => {
    if (!generatedScript) return;
    const blob = new Blob([generatedScript], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const downloadFile = (b64: string, dlFilename: string, mimeType: string) => {
    const bytes = atob(b64);
    const arr = new Uint8Array(bytes.length);
    for (let i = 0; i < bytes.length; i++) arr[i] = bytes.charCodeAt(i);
    const blob = new Blob([arr], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = dlFilename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleExecute = async () => {
    if (!generatedScript || !originalFile) return;
    setIsExecuting(true);
    setExecuteError(null);
    setExecuteStats(null);
    try {
      const { executeScript } = await import('@/lib/api');
      const result = await executeScript(originalFile, generatedScript);
      if (result.stats) setExecuteStats(result.stats);
      downloadFile(result.file_base64, result.filename, result.mimetype);
    } catch (err: any) {
      setExecuteError(err.message || "Erreur d'exécution");
    } finally {
      setIsExecuting(false);
    }
  };

  if (steps.length === 0 && !generatedScript) {
    return (
      <div className="alert">
        Cliquez sur les problèmes pour choisir des stratégies de nettoyage.
      </div>
    );
  }

  const isExcel = originalFile?.name.match(/\.(xlsx|xls)$/i);

  return (
    <div>
      {/* Steps list — only shown when steps have code content */}
      {steps.length > 0 && steps.some((s) => s.code) && (
        <>
          <h3 className="section-label">
            Pipeline de nettoyage ({steps.length} étape{steps.length > 1 ? 's' : ''})
          </h3>
          <div className="space-y-2">
            {steps.map((step, idx) => (
              <div key={idx} style={{ padding: "0.6rem 0", borderTop: "1px solid var(--rule-soft)" }}>
                <div className="flex justify-between items-center mb-2">
                  <span className="font-mono text-[0.82rem]">
                    #{idx + 1} {step.strategy_name}
                  </span>
                  {step.column && <span className="meta">{step.column}</span>}
                </div>
                <pre className="code" style={{ marginTop: "0.4rem" }}>
                  <code>{step.code}</code>
                </pre>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Generated script */}
      {generatedScript && (
        <>
          <div>
            <div className="section-head">
              <span className="section-label">{filename}</span>
              <div className="flex gap-4 flex-wrap items-baseline">
                <button
                  onClick={copyToClipboard}
                  className="btn btn--quiet"
                >
                  {copied ? 'Copié !' : 'Copier'}
                </button>
                <button
                  onClick={downloadScript}
                  className="btn btn--quiet"
                >
                  Télécharger .py
                </button>
                {originalFile && (
                  <button
                    onClick={handleExecute}
                    disabled={isExecuting}
                    className="btn btn--primary"
                  >
                    {isExecuting ? (
                      <>
                        <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                        </svg>
                        Exécution...
                      </>
                    ) : (
                      `Exécuter & Télécharger ${isExcel ? '.xlsx' : '.csv'}`
                    )}
                  </button>
                )}
                {onRegenerate && (
                  <button
                    onClick={onRegenerate}
                    className="btn btn--quiet"
                  >
                    Recommencer
                  </button>
                )}
              </div>
            </div>
            <pre className="code">
              {generatedScript}
            </pre>
          </div>

          {executeError && (
            <div className="alert alert--bad">
              {executeError}
            </div>
          )}

          {executeStats && (
            <div className="panel">
              <h4 className="section-label section-head">ce que le script a change</h4>
              <div className="grid grid-cols-3 gap-3">
                <div className="bg-white rounded-lg p-3 shadow-sm text-center">
                  <div className="text-xl font-bold text-gray-800">
                    {executeStats.rows_before.toLocaleString()}
                    <span className="text-sm text-gray-400 mx-1">→</span>
                    {executeStats.rows_after.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">Lignes</div>
                  {executeStats.rows_removed > 0 && (
                    <div className="text-xs text-orange-600">−{executeStats.rows_removed} supprimées</div>
                  )}
                </div>
                <div className="bg-white rounded-lg p-3 shadow-sm text-center">
                  <div className="text-xl font-bold text-green-600">{executeStats.null_fixed}</div>
                  <div className="text-xs text-gray-500 mt-1">Valeurs manquantes corrigées</div>
                </div>
                <div className="bg-white rounded-lg p-3 shadow-sm text-center">
                  <div className="text-xl font-bold text-blue-600">
                    {executeStats.null_before > 0
                      ? Math.round((executeStats.null_fixed / executeStats.null_before) * 100)
                      : 100}%
                  </div>
                  <div className="text-xs text-gray-500 mt-1">Taux de correction</div>
                </div>
              </div>
              <p className="text-xs text-green-700 mt-3">
                Le fichier nettoyé a été téléchargé automatiquement.
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
};
