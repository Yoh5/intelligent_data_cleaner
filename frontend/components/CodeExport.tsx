'use client';

import React, { useState } from 'react';

interface CodeStep {
  strategy_name: string;
  column?: string;
  code: string;
}

interface CodeExportProps {
  steps: CodeStep[];
  generatedScript?: string;
  filename?: string;
  onRegenerate?: () => void;
}

export const CodeExport: React.FC<CodeExportProps> = ({ 
  steps, 
  generatedScript, 
  filename = 'clean_dataset.py',
  onRegenerate 
}) => {
  const [copied, setCopied] = useState(false);

  const copyToClipboard = () => {
    if (generatedScript) {
      navigator.clipboard.writeText(generatedScript);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (steps.length === 0) {
    return (
      <div className="p-6 text-center text-gray-500">
        Aucune stratégie sélectionnée. Cliquez sur les problèmes pour choisir des solutions.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold">
        Pipeline de nettoyage ({steps.length} étape{steps.length > 1 ? 's' : ''})
      </h3>

      {/* Liste des étapes */}
      <div className="space-y-2">
        {steps.map((step, idx) => (
          <div key={idx} className="bg-gray-50 rounded p-3 border border-gray-200">
            <div className="flex justify-between items-center mb-2">
              <span className="font-medium text-gray-700">
                #{idx + 1} {step.strategy_name}
              </span>
              {step.column && (
                <span className="text-sm text-gray-500">
                  — {step.column}
                </span>
              )}
            </div>
            <pre className="text-xs bg-gray-800 text-gray-100 p-2 rounded overflow-x-auto">
              <code>{step.code}</code>
            </pre>
          </div>
        ))}
      </div>

      {/* Boutons action */}
      {!generatedScript ? (
        <button
          onClick={onRegenerate}
          className="w-full bg-blue-600 text-white py-2 px-4 rounded hover:bg-blue-700 transition-colors"
        >
          Générer le script Python
        </button>
      ) : (
        <div className="space-y-2">
          <div className="flex justify-between items-center">
            <span className="text-sm font-medium text-gray-700">{filename}</span>
            <button
              onClick={copyToClipboard}
              className="text-sm text-blue-600 hover:text-blue-800"
            >
              {copied ? 'Copié !' : 'Copier'}
            </button>
          </div>
          <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-x-auto text-sm">
            <code className="whitespace-pre-wrap">{generatedScript}</code>
          </pre>
        </div>
      )}
    </div>
  );
};