'use client';

import React, { useEffect, useState } from 'react';

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
  rationale?: string;
  onSelect: (strategy: Strategy) => void;
  onClose: () => void;
}

export const SuggestionModal: React.FC<SuggestionModalProps> = ({
  issue,
  strategies,
  recommended,
  rationale,
  onSelect,
  onClose,
}) => {
  const [selected, setSelected] = useState(Math.min(recommended, Math.max(0, strategies.length - 1)));

  // Close on Escape key
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  return (
    <div
      className="modal-veil"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="modal-card">
        {/* Header */}
        <div className="section-head">
          <h3 className="section-label">comment corriger</h3>
          <button
            onClick={onClose}
            aria-label="Fermer"
            className="btn btn--quiet"
          >
            ✕
          </button>
        </div>

        <div>
          <p className="meta mb-5">
            <span className="font-medium">{issue.type}</span>
            {issue.column && (
              <>
                {' — '}
                <code className="font-mono">{issue.column}</code>
              </>
            )}
            {!issue.column && ' — Dataset entier'}
          </p>

          {rationale && (
            <p className="note mb-5">
              
              <span className="note-who">strategie conseillee</span>
            {rationale}</p>
          )}

          {strategies.length === 0 ? (
            <div className="alert">
              Aucune stratégie disponible pour ce type de problème.
            </div>
          ) : (
            <div>
              {strategies.map((strategy, idx) => (
                <div
                  key={idx}
                  onClick={() => setSelected(idx)}
                  className={`opt ${
                    selected === idx
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <input
                      type="radio"
                      checked={selected === idx}
                      onChange={() => setSelected(idx)}
                      className="mt-1 flex-shrink-0"
                    />
                    <div className="flex-1 min-w-0">
                      <div className="flex justify-between items-start gap-2 flex-wrap">
                        <h4 className="opt-name">{strategy.name}</h4>
                        {idx === recommended && (
                          <span className="colrow-kind flex-shrink-0">conseillée</span>
                        )}
                      </div>

                      <p className="opt-for mt-1">{strategy.description}</p>

                      <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-2">
                        <div>
                          <p className="colrow-kind mb-1">ce que ça vous donne</p>
                          <ul>
                            {strategy.pros.map((pro, i) => (
                              <li key={i} className="opt-for">{pro}</li>
                            ))}
                          </ul>
                        </div>
                        <div>
                          <p className="colrow-kind mb-1">ce que ça vous coûte</p>
                          <ul>
                            {strategy.cons.map((con, i) => (
                              <li key={i} className="opt-against">{con}</li>
                            ))}
                          </ul>
                        </div>
                      </div>

                      <div className="mt-3">
                        <p className="colrow-kind mb-1">la ligne qui sera écrite</p>
                        <pre className="code">
                          <code>{strategy.code_preview}</code>
                        </pre>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-5 items-baseline"
             style={{ marginTop: '1.4rem', paddingTop: '0.8rem', borderTop: '1px solid var(--rule)' }}>
          <button onClick={onClose} className="btn btn--quiet">annuler</button>
          <button
            onClick={() => onSelect(strategies[selected])}
            disabled={strategies.length === 0}
            className="btn btn--primary"
          >
            appliquer
          </button>
        </div>
      </div>
    </div>
  );
};
