'use client';

import React, { useState } from 'react';

interface Strategy {
  name: string;
  description: string;
  pros: string[];
  cons: string[];
  code_preview: string;
  confidence: 'high' | 'medium' | 'low';
}

interface Issue {
  type: string;
  column: string | null;
  severity: string;
}

interface SuggestionModalProps {
  issue: Issue;
  strategies: Strategy[];
  recommended: number;
  onSelect: (strategy: Strategy) => void;
  onClose: () => void;
}

export const SuggestionModal: React.FC<SuggestionModalProps> = ({
  issue,
  strategies,
  recommended,
  onSelect,
  onClose
}) => {
  const [selected, setSelected] = useState(recommended);

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="border-b p-4 flex justify-between items-center">
          <h3 className="text-lg font-semibold">
            Stratégies de nettoyage
          </h3>
          <button 
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700"
          >
            ✕
          </button>
        </div>
        
        <div className="p-4">
          <p className="text-sm text-gray-600 mb-4">
            {issue.type} — {issue.column || 'Dataset entier'}
          </p>

          {/* Strategies */}
          <div className="space-y-3">
            {strategies.map((strategy, idx) => (
              <div
                key={idx}
                onClick={() => setSelected(idx)}
                className={`
                  border-2 rounded-lg p-4 cursor-pointer transition-all
                  ${selected === idx
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300'
                  }
                `}
              >
                <div className="flex items-start gap-3">
                  <input
                    type="radio"
                    checked={selected === idx}
                    onChange={() => setSelected(idx)}
                    className="w-4 h-4 text-blue-600 mt-1"
                  />
                  <div className="flex-1">
                    <div className="flex justify-between items-start">
                      <h4 className="font-semibold text-gray-900">
                        {strategy.name}
                      </h4>
                      {idx === recommended && (
                        <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded">
                          Recommandé
                        </span>
                      )}
                    </div>
                    
                    <p className="text-sm text-gray-600 mt-1">
                      {strategy.description}
                    </p>

                    {/* Pros/Cons */}
                    <div className="mt-3 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <p className="text-green-700 font-medium mb-1">✓ Avantages</p>
                        <ul className="space-y-1">
                          {strategy.pros.map((pro, i) => (
                            <li key={i} className="text-gray-600">• {pro}</li>
                          ))}
                        </ul>
                      </div>
                      <div>
                        <p className="text-red-700 font-medium mb-1">✗ Inconvénients</p>
                        <ul className="space-y-1">
                          {strategy.cons.map((con, i) => (
                            <li key={i} className="text-gray-600">• {con}</li>
                          ))}
                        </ul>
                      </div>
                    </div>

                    {/* Code preview */}
                    <div className="mt-3">
                      <p className="text-xs text-gray-500 mb-1">Code généré :</p>
                      <pre className="bg-gray-800 text-gray-100 p-2 rounded text-xs overflow-x-auto">
                        <code>{strategy.code_preview}</code>
                      </pre>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Footer */}
        <div className="border-t p-4 flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-gray-600 hover:text-gray-800"
          >
            Annuler
          </button>
          <button
            onClick={() => onSelect(strategies[selected])}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            Appliquer cette stratégie
          </button>
        </div>
      </div>
    </div>
  );
};