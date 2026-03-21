'use client';

import { useState } from 'react';
import FileUpload from '@/components/FileUpload';
import AnalysisResult from '@/components/AnalysisResult';
import CodeExport from '@/components/CodeExport';
import { 
  AnalysisResult as AnalysisResultType, 
  CleaningStep, 
  getSuggestionsBatch,
  BatchStrategyItem 
} from '@/lib/api';

export default function Home() {
  const [result, setResult] = useState<AnalysisResultType | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedSteps, setSelectedSteps] = useState<CleaningStep[]>([]);
  const [isApplyingAll, setIsApplyingAll] = useState(false);

  const handleAnalysisComplete = (data: AnalysisResultType) => {
    setResult(data);
    setError(null);
    setSelectedSteps([]);
  };

  const handleError = (message: string) => {
    setError(message);
    setResult(null);
  };

  const handleApplyStrategy = (issue: any, strategy: any) => {
    const newStep: CleaningStep = {
      column: issue.column,
      issue_type: issue.type,
      strategy_name: strategy.name,
      code: strategy.code_preview
    };
    
    setSelectedSteps(prev => [...prev, newStep]);
  };

  // ========== NOUVEAU : Appliquer toutes les suggestions ==========
  const handleApplyAllStrategies = async () => {
    if (!result || result.issues.length === 0) return;
    
    setIsApplyingAll(true);
    setError(null);

    try {
      // Générer des sample_data cohérents
      const sampleData = Array(3).fill(null).map((_, i) => {
        const row: Record<string, any> = {};
        Object.entries(result.dataset_info.column_types).forEach(([col, type]) => {
          const isNumeric = type.includes('int') || type.includes('float');
          if (isNumeric) {
            row[col] = Math.floor(Math.random() * 1000) / 10;
          } else if (type.includes('bool')) {
            row[col] = i % 2 === 0;
          } else {
            row[col] = `sample_${i}`;
          }
        });
        return row;
      });

      const batchRequest = {
        dataset_name: result.dataset_info.filename,
        column_types: result.dataset_info.column_types,
        issues: result.issues,
        sample_data: sampleData
      };

      console.log('DEBUG: Sending batch request...');
      const response = await getSuggestionsBatch(batchRequest);

      // Appliquer toutes les stratégies recommandées
      const newSteps: CleaningStep[] = [];
      
      response.results.forEach((item: BatchStrategyItem) => {
        const recommendedStrategy = item.strategies[item.recommended] || item.strategies[0];
        
        if (recommendedStrategy) {
          newSteps.push({
            column: item.issue.column,
            issue_type: item.issue.type,
            strategy_name: recommendedStrategy.name,
            code: recommendedStrategy.code_preview
          });
        }
      });

      setSelectedSteps(prev => [...prev, ...newSteps]);
      console.log(`Applied ${newSteps.length} strategies automatically`);
      
    } catch (err: any) {
      console.error('Batch application failed:', err);
      setError('Erreur lors de l\'application automatique des stratégies');
    } finally {
      setIsApplyingAll(false);
    }
  };

  return (
    <main className="min-h-screen py-12 px-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Intelligent Data Cleaner
          </h1>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Analysez, suggérez, générez. Pipeline de nettoyage de données
            assistée par IA, 100% reproductible.
          </p>
        </div>

        {/* Upload */}
        <FileUpload 
          onAnalysisComplete={handleAnalysisComplete}
          onError={handleError}
        />

        {/* Error */}
        {error && (
          <div className="max-w-2xl mx-auto mt-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800">{error}</p>
          </div>
        )}

        {/* ========== NOUVEAU : Bouton "Tout appliquer" ========== */}
        {result && result.issues.length > 0 && (
          <div className="max-w-2xl mx-auto mt-6">
            <button
              onClick={handleApplyAllStrategies}
              disabled={isApplyingAll || selectedSteps.length > 0}
              className={`
                w-full py-3 px-6 rounded-lg font-medium transition-all
                ${isApplyingAll 
                  ? 'bg-gray-100 text-gray-500 cursor-wait' 
                  : selectedSteps.length > 0
                    ? 'bg-green-100 text-green-700 cursor-not-allowed'
                    : 'bg-gradient-to-r from-purple-600 to-blue-600 text-white hover:from-purple-700 hover:to-blue-700 shadow-lg hover:shadow-xl'
                }
              `}
            >
              {isApplyingAll ? (
                <span className="flex items-center justify-center gap-2">
                  <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"/>
                  </svg>
                  Analyse et application des stratégies...
                </span>
              ) : selectedSteps.length > 0 ? (
                <span className="flex items-center justify-center gap-2">
                  <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                  Stratégies déjà appliquées ({selectedSteps.length})
                </span>
              ) : (
                <span className="flex items-center justify-center gap-2">
                  <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                  🚀 Appliquer toutes les suggestions automatiquement
                </span>
              )}
            </button>
            <p className="text-center text-sm text-gray-500 mt-2">
              ou cliquez sur les problèmes individuels ci-dessous pour choisir manuellement
            </p>
          </div>
        )}

        {/* Results + Code Export côte à côte */}
        {result && (
          <div className="mt-8 grid lg:grid-cols-3 gap-6">
            <div className="lg:col-span-2">
              <AnalysisResult 
                result={result} 
                onApplyStrategy={handleApplyStrategy}
              />
            </div>
            <div className="lg:col-span-1">
              <CodeExport
                datasetName={result.dataset_info.filename}
                steps={selectedSteps}
                onClear={() => setSelectedSteps([])}
              />
            </div>
          </div>
        )}
      </div>
    </main>
  );
}