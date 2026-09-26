'use client';

import React, { useCallback, useState } from 'react';
import { analyzeFile, autoClean, analyzeFromUrl } from '@/lib/api';

interface FileUploadProps {
  onAnalysisComplete: (result: any) => void;
  onAutopilotComplete: (result: any) => void;
  onError: (error: string) => void;
  onFileReady: (file: File) => void;
}

type Mode = 'file' | 'url';

const ALLOWED_EXTENSIONS = ['.csv', '.xlsx', '.xls'];

function isValidPublicUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

export const FileUpload: React.FC<FileUploadProps> = ({
  onAnalysisComplete,
  onAutopilotComplete,
  onError,
  onFileReady,
}) => {
  const [mode, setMode] = useState<Mode>('file');
  const [isDragging, setIsDragging] = useState(false);
  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [urlInput, setUrlInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [loadingAction, setLoadingAction] = useState('');

  const handleFileSelected = useCallback(
    (file: File) => {
      const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
      if (!ALLOWED_EXTENSIONS.includes(ext)) {
        onError(`Format non supporté: ${ext}. Utilisez: ${ALLOWED_EXTENSIONS.join(', ')}`);
        return;
      }
      if (file.size > 100 * 1024 * 1024) {
        onError('Fichier trop volumineux (max 100 MB)');
        return;
      }
      setPendingFile(file);
      onFileReady(file);
    },
    [onError, onFileReady]
  );

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) handleFileSelected(e.target.files[0]);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };
  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  };
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files?.[0]) handleFileSelected(e.dataTransfer.files[0]);
  };

  const runAnalyze = async () => {
    if (!pendingFile) return;
    setIsLoading(true);
    setLoadingAction('Analyse en cours...');
    try {
      const result = await analyzeFile(pendingFile);
      onAnalysisComplete(result);
    } catch (err: any) {
      onError(err.message || "Erreur lors de l'analyse");
    } finally {
      setIsLoading(false);
    }
  };

  const runAutopilot = async () => {
    if (!pendingFile) return;
    setIsLoading(true);
    setLoadingAction('Nettoyage automatique...');
    try {
      const result = await autoClean(pendingFile);
      onAutopilotComplete(result);
    } catch (err: any) {
      onError(err.message || 'Erreur lors du nettoyage automatique');
    } finally {
      setIsLoading(false);
    }
  };

  const runUrlAnalyze = async () => {
    const url = urlInput.trim();
    if (!url) return;
    if (!isValidPublicUrl(url)) {
      onError('URL invalide. Utilisez une URL commençant par http:// ou https://');
      return;
    }
    setIsLoading(true);
    setLoadingAction('Récupération et analyse...');
    try {
      const result = await analyzeFromUrl(url);
      onAnalysisComplete(result);
    } catch (err: any) {
      onError(err.message || "Erreur lors de l'analyse de l'URL");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="w-full">
      {/* Mode tabs */}
      <div className="flex gap-5 mb-6">
        {(['file', 'url'] as Mode[]).map((m) => (
          <button
            key={m}
            onClick={() => {
              setMode(m);
              setPendingFile(null);
              setUrlInput('');
            }}
            className="btn btn--quiet"
            style={
              mode === m
                ? { color: 'var(--ink)', textDecoration: 'underline', textDecorationThickness: '2px', textUnderlineOffset: '4px' }
                : undefined
            }
            aria-pressed={mode === m}
          >
            {m === 'file' ? 'un fichier' : 'une adresse'}
          </button>
        ))}
      </div>

      {isLoading ? (
        <div className="py-12 text-center">
          
          <p className="meta">{loadingAction}</p>
        </div>
      ) : mode === 'file' ? (
        <div className="space-y-4">
          {/* Drop zone */}
          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            className={`drop ${
              isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-blue-400'
            }`}
          >
            <input
              type="file"
              accept=".csv,.xlsx,.xls"
              onChange={handleFileChange}
              className="hidden"
              id="file-upload"
            />
            <label htmlFor="file-upload" className="cursor-pointer block">
              <svg
                className={`mx-auto h-10 w-10 mb-3 transition-colors ${
                  isDragging ? 'text-blue-500' : 'text-gray-400'
                }`}
                stroke="currentColor"
                fill="none"
                viewBox="0 0 48 48"
              >
                <path
                  d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02"
                  strokeWidth={2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              {pendingFile ? (
                <div>
                  <p className="drop-title font-mono">{pendingFile.name}</p>
                  <p className="drop-hint">
                    {(pendingFile.size / 1024).toFixed(1)} KB — cliquez pour changer
                  </p>
                </div>
              ) : (
                <>
                  <p className="drop-title">
                    {isDragging ? 'Déposez le fichier ici' : 'Glissez-déposez votre fichier ici'}
                  </p>
                  <p className="drop-hint">ou cliquez pour choisir un fichier</p>
                  <p className="drop-hint">CSV ou Excel, 100 Mo maximum</p>
                </>
              )}
            </label>
          </div>

          {/* Action buttons */}
          {pendingFile && (
            <div className="flex gap-3 flex-wrap">
              <button
                onClick={runAnalyze}
                className="btn btn--primary"
              >
                Analyser
              </button>
              <button
                onClick={runAutopilot}
                className="btn"
              >
                Nettoyage auto
              </button>
            </div>
          )}
        </div>
      ) : (
        /* URL mode */
        <div className="space-y-4">
          <div>
            <label className="section-label block mb-1">adresse du fichier</label>
            <input
              type="url"
              value={urlInput}
              onChange={(e) => setUrlInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && runUrlAnalyze()}
              placeholder="https://... ou lien Google Sheets"
              className="w-full font-mono text-[0.82rem] px-3 py-2 bg-transparent" style={{ border: '1px solid var(--rule)', borderRadius: '3px', color: 'var(--ink)' }}
            />
            <p className="drop-hint">
              Liens directs CSV/Excel, Google Sheets (export public), S3 public
            </p>
          </div>
          <button
            onClick={runUrlAnalyze}
            disabled={!urlInput.trim()}
            className="btn btn--primary"
          >
            Analyser depuis l'URL
          </button>
        </div>
      )}
    </div>
  );
};
