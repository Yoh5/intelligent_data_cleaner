/* Icônes et illustration, en SVG inline : rien à télécharger, elles
   prennent la couleur du texte (currentColor) et suivent le mode sombre.
   Trait de 1,75 px, coins arrondis — une seule famille dans toute la page. */

import React from 'react';

type P = { size?: number; className?: string };

const Base = ({ size = 20, className, children }: P & { children: React.ReactNode }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor"
       strokeWidth={1.75} strokeLinecap="round" strokeLinejoin="round"
       className={className} aria-hidden="true">
    {children}
  </svg>
);

export const IconDatabase = (p: P) => (
  <Base {...p}>
    <ellipse cx="12" cy="5" rx="8" ry="3" />
    <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
    <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
  </Base>
);

export const IconModel = (p: P) => (
  <Base {...p}>
    <circle cx="5" cy="6" r="2" /><circle cx="5" cy="18" r="2" />
    <circle cx="12" cy="12" r="2" /><circle cx="19" cy="7" r="2" /><circle cx="19" cy="17" r="2" />
    <path d="M7 7l3 3.5M7 17l3-3.5M14 11l3-2.5M14 13l3 2.5" />
  </Base>
);

export const IconDashboard = (p: P) => (
  <Base {...p}>
    <rect x="3" y="3" width="18" height="18" rx="2" />
    <path d="M7 16v-3M12 16V8M17 16v-5" />
  </Base>
);

export const IconJoin = (p: P) => (
  <Base {...p}>
    <rect x="2.5" y="4" width="8" height="16" rx="1.5" />
    <rect x="13.5" y="4" width="8" height="16" rx="1.5" />
    <path d="M2.5 9h8M13.5 9h8M10.5 14h3" />
  </Base>
);

export const IconUpload = (p: P) => (
  <Base {...p}>
    <path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z" />
    <path d="M14 3v5h5M12 18v-6M9.5 14.5 12 12l2.5 2.5" />
  </Base>
);

export const IconFile = (p: P) => (
  <Base {...p}>
    <path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z" />
    <path d="M14 3v5h5M9 13h6M9 17h4" />
  </Base>
);

export const IconCheck = (p: P) => (
  <Base {...p}><path d="M20 6 9 17l-5-5" /></Base>
);

export const IconX = (p: P) => (
  <Base {...p}><path d="M18 6 6 18M6 6l12 12" /></Base>
);

/** Le refus : un geste délibéré, pas une erreur — d'où un signe d'interdiction, en ambre et non en rouge. */
export const IconRefuse = (p: P) => (
  <Base {...p}>
    <circle cx="12" cy="12" r="9" />
    <path d="M5.6 5.6l12.8 12.8" />
  </Base>
);

export const IconAlert = (p: P) => (
  <Base {...p}>
    <path d="M12 3 2 20h20z" /><path d="M12 10v4M12 17h.01" />
  </Base>
);

export const IconDownload = (p: P) => (
  <Base {...p}><path d="M12 4v11M7 10l5 5 5-5M5 20h14" /></Base>
);

export const IconShield = (p: P) => (
  <Base {...p}>
    <path d="M12 3 5 6v6c0 4.4 3 7.6 7 9 4-1.4 7-4.6 7-9V6z" />
    <path d="m9 12 2 2 4-4" />
  </Base>
);

/* ── L'illustration du haut de page ───────────────────────────────────
   Le produit en une image : un fichier entre, passe le contrôle, et
   seules les colonnes qui tiennent le contrat entrent en base. Une
   colonne est retenue au contrôle : c'est le refus, qui fait partie du
   résultat. Les couleurs d'état sont les mêmes que dans le rapport. */
export const HeroArt = () => (
  <svg viewBox="0 0 460 300" className="w-full h-auto" role="img"
       aria-label="Un fichier passe un contrôle de contrat : les colonnes conformes entrent en base, une colonne est refusée.">
    {/* fichier source */}
    <g>
      <rect x="10" y="40" width="130" height="190" rx="8" fill="var(--card)" stroke="var(--rule)" strokeWidth="1.5" />
      <text x="24" y="66" fontFamily="var(--font-mono)" fontSize="11" fill="var(--ink-3)">ventes.csv</text>
      {[0, 1, 2, 3, 4].map((i) => (
        <g key={i}>
          <rect x="24" y={82 + i * 28} width="102" height="18" rx="4"
                fill={i === 3 ? 'var(--hold-weak)' : 'var(--sheet-2)'}
                stroke={i === 3 ? 'var(--hold)' : 'none'} strokeWidth="1" />
          <text x="32" y={95 + i * 28} fontFamily="var(--font-mono)" fontSize="10"
                fill={i === 3 ? 'var(--hold)' : 'var(--ink-2)'}>
            {['id_client', 'montant', 'ville', 'remise', 'date'][i]}
          </text>
        </g>
      ))}
    </g>

    {/* flux */}
    <path d="M148 135 H182" stroke="var(--rule)" strokeWidth="2" strokeDasharray="4 4" />
    <path d="M178 130 l6 5 -6 5" fill="none" stroke="var(--ink-3)" strokeWidth="1.75" strokeLinecap="round" />

    {/* le contrôle */}
    <g>
      <rect x="190" y="70" width="110" height="130" rx="8" fill="var(--card)" stroke="var(--stamp)" strokeWidth="1.75" />
      <rect x="190" y="70" width="110" height="26" rx="8" fill="var(--stamp)" />
      <rect x="190" y="88" width="110" height="8" fill="var(--stamp)" />
      <text x="204" y="88" fontFamily="var(--font-sans)" fontSize="11" fontWeight="600" fill="var(--card)">Contrat</text>
      {[0, 1, 2, 3].map((i) => {
        const refus = i === 3;
        const y = 112 + i * 21;
        return (
          <g key={i}>
            {refus ? (
              <g stroke="var(--hold)" strokeWidth="1.75" fill="none" strokeLinecap="round">
                <circle cx="210" cy={y} r="5.5" /><path d={`M206 ${y - 4} l8 8`} />
              </g>
            ) : (
              <path d={`M205.5 ${y} l3 3 6 -6`} stroke="var(--ok)" strokeWidth="2" fill="none"
                    strokeLinecap="round" strokeLinejoin="round" />
            )}
            <rect x="224" y={y - 4} width={refus ? 48 : 62 - i * 6} height="7" rx="3.5"
                  fill={refus ? 'var(--hold-weak)' : 'var(--sheet-3)'} />
          </g>
        );
      })}
    </g>

    {/* vers la base */}
    <path d="M308 120 H342" stroke="var(--ok)" strokeWidth="2" />
    <path d="M338 115 l6 5 -6 5" fill="none" stroke="var(--ok)" strokeWidth="1.75" strokeLinecap="round" />

    {/* la base */}
    <g fill="var(--card)" stroke="var(--stamp)" strokeWidth="1.75">
      <path d="M352 82 v76 c0 9 18 16 44 16 s44 -7 44 -16 v-76" />
      <path d="M352 120 c0 9 18 16 44 16 s44 -7 44 -16" fill="none" />
      <ellipse cx="396" cy="82" rx="44" ry="15" />
    </g>
    <text x="396" y="200" textAnchor="middle" fontFamily="var(--font-mono)" fontSize="10" fill="var(--ink-3)">base de données</text>

    {/* le refus, consigné */}
    <path d="M245 200 V236" stroke="var(--hold)" strokeWidth="1.75" strokeDasharray="3 4" />
    <rect x="186" y="240" width="118" height="40" rx="8" fill="var(--hold-weak)" stroke="var(--hold)" strokeWidth="1.25" />
    <text x="245" y="257" textAnchor="middle" fontFamily="var(--font-sans)" fontSize="10.5" fontWeight="600" fill="var(--hold)">Refusé, et consigné</text>
    <text x="245" y="271" textAnchor="middle" fontFamily="var(--font-mono)" fontSize="9" fill="var(--hold)">remise : 41 % vide</text>
  </svg>
);
