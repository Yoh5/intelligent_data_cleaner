'use client';

import { useState } from 'react';
import { IssueDetected, CleaningStrategy, SuggestionResponse } from '@/lib/api';

interface SuggestionModalProps {
  isOpen: boolean;
  onClose: () => void;
  issue: IssueDetected | null;
  strategies: CleaningStrategy[];
  recommended: number;
  onApply: (strategy: CleaningStrategy) => void;
}

export default function SuggestionModal({
  isOpen,
  onClose,
  issue,
  strategies,
  recommended,
  onApply
}: SuggestionModalProps) {
  const [selected, setSelected] = useState(recommended);

  if (!isOpen || !issue) return null;

  const confidenceColors = {
    high: 'bg-green-100 text-green-800',
    medium: 'bg-yellow-100 text-yellow-800',
    low: 'bg-gray-100 text-gray-800'
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="p-6 border-b">
          <div className="flex justify-between items-start">
            <div>
              <h3 className="text-xl font-bold mb-2">
                Stratégies de nettoyage
              </h3>
              <p className="text-gray-600">
                {issue.type} — {issue.column || 'Dataset entier'}
              </p>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Strategies */}
        <div className="p-6 space-y-4">
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
              <div className="flex items-start justify-between mb-2">
                <div className="flex items-center gap-2">
                  <input
                    type="radio"
                    checked={selected === idx}
                    onChange={() => setSelected(idx)}
                    className="w-4 h-4 text-blue-600"
                  />
                  <h4 className="font-semibold">{strategy.name}</h4>
                  {idx === recommended && (
                    <span className="bg-blue-100 text-blue-700 text-xs px-2 py-1 rounded-full">
                      Recommandé
                    </span>
                  )}
                </div>
                <span className={`text-xs px-2 py-1 rounded-full ${confidenceColors[strategy.confidence]}`}>
                  {strategy.confidence}
                </span>
              </div>

              <p className="text-gray-600 text-sm mb-3">
                {strategy.description}
              </p>

              {/* Pros/Cons */}
              <div className="grid grid-cols-2 gap-4 text-sm mb-3">
                <div>
                  <p className="text-green-600 font-medium mb-1">✓ Avantages</p>
                  <ul className="space-y-1">
                    {strategy.pros.map((pro, i) => (
                      <li key={i} className="text-gray-600">• {pro}</li>
                    ))}
                  </ul>
                </div>
                <div>
                  <p className="text-red-600 font-medium mb-1">✗ Inconvénients</p>
                  <ul className="space-y-1">
                    {strategy.cons.map((con, i) => (
                      <li key={i} className="text-gray-600">• {con}</li>
                    ))}
                  </ul>
                </div>
              </div>

              {/* Code preview */}
              <div className="bg-gray-900 rounded p-3">
                <p className="text-gray-400 text-xs mb-2">Code généré :</p>
                <code className="text-green-400 text-sm font-mono block">
                  {strategy.code_preview}
                </code>
              </div>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="p-6 border-t bg-gray-50 flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-gray-600 hover:text-gray-800"
          >
            Annuler
          </button>
          <button
            onClick={() => onApply(strategies[selected])}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            Appliquer cette stratégie
          </button>
        </div>
      </div>
    </div>
  );
}