'use client';

import { useState, useCallback } from 'react';
import { analyzeFile, AnalysisResult } from '@/lib/api';

interface FileUploadProps {
  onAnalysisComplete: (result: AnalysisResult) => void;
  onError: (error: string) => void;
}

export default function FileUpload({ onAnalysisComplete, onError }: FileUploadProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState('');

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      handleFile(files[0]);
    }
  }, []);

  const handleFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  }, []);

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
    <div className="w-full max-w-2xl mx-auto">
      <div
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        className={`
          border-2 border-dashed rounded-lg p-8 text-center transition-colors
          ${isDragging 
            ? 'border-blue-500 bg-blue-50' 
            : 'border-gray-300 hover:border-gray-400'
          }
          ${isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
        `}
      >
        <input
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={handleFileInput}
          disabled={isLoading}
          className="hidden"
          id="file-input"
        />
        
        <label htmlFor="file-input" className="cursor-pointer block">
          <div className="mb-4">
            <svg 
              className="mx-auto h-12 w-12 text-gray-400"
              stroke="currentColor"
              fill="none"
              viewBox="0 0 48 48"
            >
              <path 
                d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02" 
                strokeWidth="2" 
                strokeLinecap="round" 
                strokeLinejoin="round"
              />
            </svg>
          </div>
          
          {isLoading ? (
            <div>
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto mb-4"></div>
              <p className="text-blue-600 font-medium">{progress}</p>
            </div>
          ) : (
            <>
              <p className="text-lg font-medium text-gray-900 mb-2">
                Glissez-déposez votre fichier ici
              </p>
              <p className="text-sm text-gray-500 mb-4">
                ou cliquez pour sélectionner
              </p>
              <p className="text-xs text-gray-400">
                CSV, Excel (max 100MB)
              </p>
            </>
          )}
        </label>
      </div>
    </div>
  );
}