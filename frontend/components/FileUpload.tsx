'use client';

import React, { useCallback, useState } from 'react';

interface FileUploadProps {
  onAnalysisComplete: (result: any) => void;
  onError: (error: string) => void;
}

export const FileUpload: React.FC<FileUploadProps> = ({ 
  onAnalysisComplete, 
  onError 
}) => {
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState('');

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      await handleFile(files[0]);
    }
  };

  const handleFile = async (file: File) => {
    // Validation
    const allowedExtensions = ['.csv', '.xlsx', '.xls'];
    const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));

    if (!allowedExtensions.includes(ext)) {
      onError(`Format non supporté: ${ext}. Utilisez: ${allowedExtensions.join(', ')}`);
      return;
    }

    if (file.size > 100 * 1024 * 1024) {
      onError('Fichier trop volumineux (max 100MB)');
      return;
    }

    setIsLoading(true);
    setProgress('Analyse en cours...');

    try {
      // Import dynamique pour éviter les problèmes SSR
      const { analyzeFile } = await import('@/lib/api');
      const result = await analyzeFile(file);
      onAnalysisComplete(result);
    } catch (err: any) {
      onError(err.response?.data?.detail || 'Erreur lors de l\'analyse');
    } finally {
      setIsLoading(false);
      setProgress('');
    }
  };

  return (
    <div className="w-full max-w-2xl mx-auto p-6">
      <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center hover:border-blue-500 transition-colors">
        <input
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={handleFileChange}
          className="hidden"
          id="file-upload"
          disabled={isLoading}
        />
        
        <label 
          htmlFor="file-upload"
          className={`cursor-pointer block ${isLoading ? 'opacity-50' : ''}`}
        >
          {isLoading ? (
            <div className="space-y-2">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
              <p className="text-gray-600">{progress}</p>
            </div>
          ) : (
            <>
              <svg 
                className="mx-auto h-12 w-12 text-gray-400 mb-4"
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
              <p className="text-lg font-medium text-gray-700">
                Glissez-déposez votre fichier ici
              </p>
              <p className="text-sm text-gray-500 mt-1">
                ou cliquez pour sélectionner
              </p>
              <p className="text-xs text-gray-400 mt-2">
                CSV, Excel (max 100MB)
              </p>
            </>
          )}
        </label>
      </div>
    </div>
  );
};