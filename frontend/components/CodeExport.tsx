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
  onRegenerate,
}) => {
  const [copied, setCopied] = useState(false);

  const copyToClipboard = () => {
    if (generatedScript) {
      navigator.clipboard.writeText(generatedScript);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const download = () => {
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

  if (steps.length === 0 && !generatedScript) {
    return (
      <div className="p-6 text-center text-gray-500">
        Cliquez sur les problèmes pour choisir des stratégies de nettoyage.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-gray-800">
        Pipeline de nettoyage ({steps.length} étape{steps.length > 1 ? 's' : ''})
      </h3>

      <div className="space-y-2">
        {steps.map((step, idx) => (
          <div key={idx} className="bg-gray-50 rounded p-3 border border-gray-200">
            <div className="flex justify-between items-center mb-2">
              <span className="font-medium text-gray-700">
                #{idx + 1} {step.strategy_name}
              </span>
              {step.column && (
                <span className="text-sm text-gray-500">— {step.column}</span>
              )}
            </div>
            <pre className="text-xs bg-gray-800 text-gray-100 p-2 rounded overflow-x-auto">
              <code>{step.code}</code>
            </pre>
          </div>
        ))}
      </div>

      {!generatedScript ? (
        <button
          onClick={onRegenerate}
          className="w-full bg-blue-600 text-white py-2 px-4 rounded hover:bg-blue-700 transition-colors"
        >
          Générer le script Python
        </button>
      ) : (
        <div className="space-y-3">
          <div className="bg-gray-900 rounded-xl overflow-hidden shadow-2xl">
            <div className="bg-gray-800 px-4 py-3 flex justify-between items-center">
              <span className="text-gray-200 font-mono text-sm">{filename}</span>
              <div className="flex gap-2">
                <button
                  onClick={copyToClipboard}
                  className="text-xs bg-gray-700 hover:bg-gray-600 text-gray-200 px-3 py-1 rounded transition-colors"
                >
                  {copied ? 'Copié !' : 'Copier'}
                </button>
                <button
                  onClick={download}
                  className="text-xs bg-green-700 hover:bg-green-600 text-white px-3 py-1 rounded transition-colors"
                >
                  Télécharger
                </button>
                {onRegenerate && (
                  <button
                    onClick={onRegenerate}
                    className="text-xs bg-blue-700 hover:bg-blue-600 text-white px-3 py-1 rounded transition-colors"
                  >
                    Recommencer
                  </button>
                )}
              </div>
            </div>
            <pre className="p-4 overflow-x-auto text-sm text-gray-300 font-mono leading-relaxed max-h-[500px] overflow-y-auto">
              {generatedScript}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
};
