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

const TYPE_LABELS: Record<string, string> = {
  missing: 'valeurs manquantes',
  missing_values: 'valeurs manquantes',
  outlier: 'valeurs aberrantes',
  inconsistent: 'types inconsistants',
  duplicate: 'doublons',
  duplicate_ids: 'doublons d’identifiant',
  duplicate_candidates: 'doublons probables',
  pii: 'données personnelles',
  mixed_types: 'types mélangés',
};

const SEVERITY_LABELS: Record<string, string> = {
  critical: 'critique',
  high: 'élevé',
  medium: 'moyen',
  low: 'faible',
};

/**
 * Une colonne du fichier, sur une ligne.
 *
 * Pas une carte : la carte force chaque problème à occuper la même
 * surface, ce qui dit au lecteur que tout se vaut. Ici le nom de la
 * colonne est en mono — c'est un identifiant de code, il finira écrit
 * tel quel dans le script — et la bande de densité montre la proportion
 * réelle de cellules touchées. On lit la liste et on VOIT où sont les
 * trous, au lieu de lire « 23 problèmes » dans une tuile.
 */
export const IssueCard: React.FC<IssueCardProps> = ({ issue, index, onClick, isSelected = false }) => {
  const kind = issue.type || issue.issue || 'inconnu';
  const label = TYPE_LABELS[kind] || kind.replace(/_/g, ' ');
  const severity = SEVERITY_LABELS[issue.severity] || issue.severity;

  // La part touchée, telle qu'elle est mesurée. Pas de valeur inventée :
  // sans taux connu, la bande n'est pas dessinée plutôt que de mentir.
  const rate = typeof issue.rate === 'number' ? Math.min(Math.max(issue.rate, 0), 1) : null;
  const touched = issue.affected_rows ?? issue.count ?? null;

  const Tag = onClick ? 'button' : 'div';

  return (
    <Tag
      {...(onClick ? { type: 'button' as const, onClick: () => onClick(issue) } : {})}
      className={[
        'colrow',
        onClick ? 'colrow--pick' : '',
        isSelected ? 'colrow--on' : '',
      ].join(' ')}
      aria-label={issue.column ? `Colonne ${issue.column} — ${label}` : label}
    >
      <span className="colrow-name">{issue.column || `—`}</span>

      <span className="colrow-kind">
        {issue.semantic_type ? `${issue.semantic_type} · ` : ''}{label}
      </span>

      {rate !== null ? (
        <span className="strip" role="img" aria-label={`${(rate * 100).toFixed(0)}% des lignes touchées`}>
          <i style={{ width: `${rate * 100}%` }} />
        </span>
      ) : (
        <span />
      )}

      <span className="colrow-note">
        {rate !== null && `${(rate * 100).toFixed(rate < 0.01 ? 2 : 0)} %`}
        {rate !== null && touched !== null && ' · '}
        {touched !== null && `${touched.toLocaleString('fr-FR')} lignes`}
        <span className="meta-sep">·</span>
        <span className={`sev sev--${issue.severity}`}>{severity}</span>
      </span>

      <span className="colrow-desc">
        <span>{issue.description}</span>
        {onClick && <span className="colrow-hint">choisir une stratégie</span>}
      </span>
    </Tag>
  );
};
