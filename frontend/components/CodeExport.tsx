'use client';

import { useState } from 'react';
import { CleaningStep, generateCode } from '@/lib/api';

interface CodeExportProps {
  datasetName: string;
  steps: CleaningStep[];
  onClear: () => void;
}

export default function CodeExport({ datasetName, steps, onClear }: CodeExportProps) {
  const [isGenerating, setIsGenerating] = useState(false);
  const [generatedScript, setGeneratedScript] = useState<string | null>(null);
  const [filename, setFilename] = useState('');

  const handleGenerate = async () => {
    if (steps.length === 0) return;
    
    setIsGenerating(true);
    try {
      const response = await generateCode({
        dataset_name: datasetName,
        steps
      });
      setGeneratedScript(response.script);
      setFilename(response.filename);
    } catch (err) {
      alert('Erreur génération code');
    } finally {
      setIsGenerating(false);
    }
  };

  const handleDownload = () => {
    if (!generatedScript) return;
    
    const blob = new Blob([generatedScript], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleCopy = () => {
    if (!generatedScript) return;
    navigator.clipboard.writeText(generatedScript);
    alert('Code copié dans le presse-papier !');
  };

  if (steps.length === 0) {
    return (
      <div className="bg-gray-50 rounded-lg p-6 text-center text-gray-500">
        Aucune stratégie sélectionnée. Cliquez sur les problèmes pour choisir des solutions.
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow p-6 space-y-4">
      <div className="flex items-center justify-between">
        // Dans CodeExport.tsx, changez le titre :
        <h3 className="text-lg font-semibold">
        Pipeline de nettoyage ({steps.length} étape{steps.length > 1 ? 's' : ''})
        </h3>
        <button
          onClick={onClear}
          className="text-red-600 hover:text-red-800 text-sm"
        >
          Tout effacer
        </button>
      </div>

      {/* Liste des étapes */}
      <div className="space-y-2 max-h-60 overflow-y-auto">
        {steps.map((step, idx) => (
          <div key={idx} className="bg-gray-50 rounded p-3 text-sm">
            <div className="flex items-center gap-2">
              <span className="bg-blue-100 text-blue-800 px-2 py-1 rounded text-xs">
                #{idx + 1}
              </span>
              <span className="font-medium">{step.strategy_name}</span>
              {step.column && (
                <span className="text-gray-500">— {step.column}</span>
              )}
            </div>
            <code className="text-xs text-gray-600 block mt-1 truncate">
              {step.code}
            </code>
          </div>
        ))}
      </div>

      {/* Boutons action */}
      {!generatedScript ? (
        <button
          onClick={handleGenerate}
          disabled={isGenerating}
          className="w-full py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {isGenerating ? 'Génération...' : 'Générer le script Python'}
        </button>
      ) : (
        <div className="space-y-3">
          <div className="bg-gray-900 rounded-lg p-4 max-h-80 overflow-y-auto">
            <div className="flex justify-between items-center mb-2">
              <span className="text-gray-400 text-sm">{filename}</span>
              <button
                onClick={handleCopy}
                className="text-green-400 hover:text-green-300 text-sm"
              >
                Copier
              </button>
            </div>
            <pre className="text-green-400 text-sm font-mono whitespace-pre-wrap">
              {generatedScript}
            </pre>
          </div>
          
          <div className="flex gap-3">
            <button
              onClick={handleDownload}
              className="flex-1 py-2 bg-green-600 text-white rounded hover:bg-green-700"
            >
              Télécharger .py
            </button>
            <button
              onClick={() => setGeneratedScript(null)}
              className="flex-1 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300"
            >
              Régénérer
            </button>
          </div>
        </div>
      )}
    </div>
  );
}