import { IssueDetected } from '@/lib/api';

interface IssueCardProps {
  issue: IssueDetected;
  index: number;
  onClick?: (issue: IssueDetected) => void;  // ← AJOUTÉ
}

export default function IssueCard({ issue, index, onClick }: IssueCardProps) {
  const severityColors = {
    high: 'bg-red-100 border-red-300 text-red-800 hover:border-red-400',
    medium: 'bg-yellow-100 border-yellow-300 text-yellow-800 hover:border-yellow-400',
    low: 'bg-blue-100 border-blue-300 text-blue-800 hover:border-blue-400'
  };

  const typeLabels: Record<string, string> = {
    missing: 'Valeurs manquantes',
    outlier: 'Valeurs aberrantes',
    inconsistent: 'Types inconsistants',
    duplicate: 'Doublons',
    duplicate_candidates: 'Candidats doublons',
    pii: 'Données sensibles (PII)',
  };

  return (
    <div 
      onClick={() => onClick?.(issue)}  // ← AJOUTÉ
      className={`
        border-2 rounded-lg p-4 transition-all cursor-pointer
        ${severityColors[issue.severity]}
        ${onClick ? 'hover:shadow-md' : ''}
      `}
    >
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-xs font-bold uppercase tracking-wide opacity-75">
              {issue.severity}
            </span>
            <span className="text-xs opacity-75">
              #{index + 1}
            </span>
            {onClick && (
              <span className="text-xs opacity-50 ml-auto">
                Cliquez pour suggestions →
              </span>
            )}
          </div>
          
          <h4 className="font-semibold mb-1">
            {typeLabels[issue.type] || issue.type}
          </h4>
          
          {issue.column && (
            <p className="text-sm opacity-75 mb-2">
              Colonne: <code className="bg-black/10 px-1 rounded">{issue.column}</code>
            </p>
          )}
          
          <p className="text-sm">{issue.description}</p>
          
          {issue.affected_rows && (
            <p className="text-sm mt-2 font-medium">
              {issue.affected_rows.toLocaleString()} ligne(s) concernée(s)
            </p>
          )}
        </div>
      </div>
    </div>
  );
}