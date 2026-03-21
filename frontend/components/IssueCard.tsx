import React from 'react';

export interface Issue {
  type: string;
  severity: 'high' | 'medium' | 'low' | 'critical';
  column: string | null;
  description: string;
  affected_rows: number | null;
  count?: number;
  rate?: number;
  semantic_type?: string;
  issue?: string;  // Pour compatibilité avec AnalysisResult
}

interface IssueCardProps {
  issue: Issue;
  index: number;
  onClick?: (issue: Issue) => void;
  isSelected?: boolean;
}

export const IssueCard: React.FC<IssueCardProps> = ({ 
  issue, 
  index, 
  onClick,
  isSelected = false
}) => {
  const severityColors = {
    critical: 'border-red-500 bg-red-50',
    high: 'border-orange-500 bg-orange-50',
    medium: 'border-yellow-500 bg-yellow-50',
    low: 'border-blue-500 bg-blue-50'
  };

  const typeLabels: Record<string, string> = {
    missing: 'Valeurs manquantes',
    missing_values: 'Valeurs manquantes',
    outlier: 'Valeurs aberrantes',
    inconsistent: 'Types inconsistants',
    duplicate: 'Doublons',
    duplicate_ids: 'Doublons ID',
    duplicate_candidates: 'Candidats doublons',
    pii: 'Données sensibles (PII)',
    mixed_types: 'Types mixtes'
  };

  // Déterminer le label à afficher
  const displayType = issue.type || issue.issue || 'unknown';
  const label = typeLabels[displayType] || displayType;

  return (
    <div
      onClick={() => onClick?.(issue)}
      className={`
        border-2 rounded-lg p-4 transition-all cursor-pointer
        ${severityColors[issue.severity] || 'border-gray-200'}
        ${onClick ? 'hover:shadow-md' : ''}
        ${isSelected ? 'ring-2 ring-blue-500' : ''}
      `}
    >
      <div className="flex justify-between items-start mb-2">
        <div className="flex items-center gap-2">
          <span className="text-xs font-bold uppercase tracking-wide text-gray-500">
            #{index + 1}
          </span>
          <span className={`px-2 py-1 rounded text-xs font-semibold ${
            issue.severity === 'critical' ? 'bg-red-200 text-red-800' :
            issue.severity === 'high' ? 'bg-orange-200 text-orange-800' :
            issue.severity === 'medium' ? 'bg-yellow-200 text-yellow-800' :
            'bg-blue-200 text-blue-800'
          }`}>
            {issue.severity}
          </span>
        </div>
        {onClick && (
          <span className="text-blue-600 text-sm font-medium">
            Cliquez pour suggestions →
          </span>
        )}
      </div>

      <h4 className="font-semibold text-gray-900 mb-1">
        {label}
      </h4>

      {issue.column && (
        <p className="text-sm text-gray-600 mb-2">
          Colonne: <code className="bg-gray-100 px-1 rounded">{issue.column}</code>
          {issue.semantic_type && (
            <span className="ml-2 text-xs bg-gray-200 px-2 py-0.5 rounded">
              {issue.semantic_type}
            </span>
          )}
        </p>
      )}

      <p className="text-sm text-gray-700 mb-2">
        {issue.description}
      </p>

      {issue.affected_rows && (
        <p className="text-xs text-gray-500">
          {issue.affected_rows.toLocaleString()} ligne(s) concernée(s)
          {issue.rate && ` (${(issue.rate * 100).toFixed(1)}%)`}
        </p>
      )}
      
      {issue.count && (
        <p className="text-xs text-gray-500">
          {issue.count} valeur(s) manquante(s)
          {issue.rate && ` (${(issue.rate * 100).toFixed(1)}%)`}
        </p>
      )}
    </div>
  );
};