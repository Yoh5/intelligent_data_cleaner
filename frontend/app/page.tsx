'use client';

import { useState } from 'react';
import { FileUpload } from '@/components/FileUpload';
import { AnalysisResult } from '@/components/AnalysisResult';

export default function Home() {
  const [analysis, setAnalysis] = useState<any>(null);
  const [originalData, setOriginalData] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleAnalysisComplete = (result: any) => {
    setAnalysis(result);
    setError(null);
    
    // Extraction des données brutes pour l'aperçu (si disponible dans raw_profile)
    if (result.raw_profile && result.raw_profile.sample_data) {
      setOriginalData(result.raw_profile.sample_data);
    } else {
      // Générer des données fictives pour la démo si pas de données réelles
      setOriginalData([]);
    }
  };

  const handleGenerateCode = (code: string) => {
    console.log('Code généré:', code);
  };

  const reset = () => {
    setAnalysis(null);
    setOriginalData([]);
    setError(null);
  };

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="max-w-6xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-extrabold text-gray-900 sm:text-5xl mb-4">
            Intelligent Data Cleaner
          </h1>
          <p className="text-xl text-gray-600 max-w-2xl mx-auto">
            Analysez et nettoyez vos données automatiquement. Détection des valeurs manquantes, 
            types inconsistants et génération de code Python optimisé.
          </p>
        </div>

        {!analysis ? (
          <div className="bg-white shadow-xl rounded-2xl p-8 max-w-2xl mx-auto">
            <FileUpload 
              onAnalysisComplete={handleAnalysisComplete}
              onError={(err) => setError(err)}
            />
            {error && (
              <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
                <p className="text-red-800 text-center">{error}</p>
              </div>
            )}
          </div>
        ) : (
          <div>
            <button
              onClick={reset}
              className="mb-6 text-blue-600 hover:text-blue-800 font-medium"
            >
              ← Analyser un autre fichier
            </button>
            <AnalysisResult
              analysis={{
                ...analysis,
                shape: [analysis.dataset_info?.rows || 0, analysis.dataset_info?.columns || 0],
                issues: analysis.issues || []
              }}
              originalData={originalData}
              onGenerateCode={handleGenerateCode}
            />
          </div>
        )}
      </div>
    </main>
  );
}