'use client';

/* Le parcours principal : un fichier, un usage, un contrat, un rapport.

   1. Déposer le fichier.
   2. Dire ce qu'on va en faire : l'usage fixe ce qui est exigé.
   3. Lire le contrat proposé et ses arbitrages, AVANT que rien ne change.
   4. Lancer la passe, et lire le rapport en trois colonnes :
      garanti / non garanti / refusé exprès.
   5. Repartir avec les données, le Parquet typé et le script. */

import React, { useCallback, useRef, useState } from 'react';
import {
  Contrat, PassageContrat, PlanPreparation, Proposition, UsageKey,
  passerContrat, preparerChaine, proposerContrat, telecharger,
} from '@/lib/api';
import {
  IconAlert, IconCheck, IconDashboard, IconDatabase, IconDownload, IconFile,
  IconJoin, IconModel, IconRefuse, IconShield, IconUpload, IconX,
} from './Illustrations';

const MAX_MO = 25;
const EXTENSIONS = ['.csv', '.xlsx', '.xls'];

const USAGES: { cle: UsageKey; nom: string; regle: string; Icone: (p: { size?: number }) => React.ReactElement }[] = [
  { cle: 'base', nom: 'Charger en base', regle: '0 % inventé · 0 ligne perdue · clé exigée', Icone: IconDatabase },
  { cle: 'jointure', nom: 'Joindre à une table', regle: '≤ 5 % inventé · 0 ligne perdue · clé exigée', Icone: IconJoin },
  { cle: 'modele', nom: 'Entraîner un modèle', regle: '≤ 10 % inventé · ≤ 5 % de lignes perdues', Icone: IconModel },
  { cle: 'tableau', nom: 'Tableau de bord', regle: '≤ 20 % inventé · ≤ 10 % de lignes perdues', Icone: IconDashboard },
];

const pct = (x: number | null | undefined) =>
  x === null || x === undefined ? '—' : `${Math.round(x * 1000) / 10} %`;
const nombre = (n: number) => n.toLocaleString('fr-FR');

/** « « age » est vide… » -> met le nom de colonne en évidence. */
const AvecColonnes = ({ texte }: { texte: string }) => {
  // « nom » sans espace = un nom de colonne ; avec espaces, c'est une citation
  // (le libellé d'un usage), qui reste du texte.
  const morceaux = texte.split(/(« [^»]+ »)/g);
  return (
    <span>
      {morceaux.map((m, i) => {
        const nom = m.startsWith('«') ? m.slice(2, -2) : null;
        return nom && !nom.includes(' ')
          ? <span key={i} className="col-ref">{nom}</span>
          : <span key={i}>{nom ?? m}</span>;
      })}
    </span>
  );
};

/** « age : 177 vides… » (format du journal) -> le nom de colonne en tête. */
const LigneJournal = ({ texte }: { texte: string }) => {
  const i = texte.indexOf(' : ');
  if (i <= 0 || texte.slice(0, i).includes(' ')) return <AvecColonnes texte={texte} />;
  return <span><span className="col-ref">{texte.slice(0, i)}</span> — {texte.slice(i + 3)}</span>;
};

/** Les arbitrages qui sont des informations (clé proposée, budgets), pas des alertes. */
const estInformation = (a: string) => /^(clé primaire proposée|budgets pour)/.test(a);

/* ── Étape 1 : le fichier ─────────────────────────────────────────── */

function Depot({ fichier, onFichier, onErreur }: {
  fichier: File | null; onFichier: (f: File | null) => void; onErreur: (m: string) => void;
}) {
  const [survol, setSurvol] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  const choisir = useCallback((f: File) => {
    const ext = f.name.toLowerCase().slice(f.name.lastIndexOf('.'));
    if (!EXTENSIONS.includes(ext)) return onErreur(`Format non pris en charge : ${ext}. Formats acceptés : CSV, XLSX, XLS.`);
    if (f.size > MAX_MO * 1024 * 1024)
      return onErreur(`Fichier de ${Math.round(f.size / 1048576)} Mo : la passe sous contrat accepte ${MAX_MO} Mo au plus (débit mesuré : 1,8 Mo/s).`);
    onFichier(f);
  }, [onFichier, onErreur]);

  if (fichier) {
    return (
      <div className="file-chip">
        <span className="flex items-center gap-3">
          <IconFile size={22} />
          <span>
            <span className="col-ref">{fichier.name}</span>
            <span className="meta block">{(fichier.size / 1024).toFixed(0)} Ko</span>
          </span>
        </span>
        <button className="btn btn--quiet" onClick={() => onFichier(null)}>changer de fichier</button>
      </div>
    );
  }

  return (
    <div
      role="button" tabIndex={0}
      className={`drop ${survol ? 'drop--over' : ''}`}
      onClick={() => input.current?.click()}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); input.current?.click(); } }}
      onDragOver={(e) => { e.preventDefault(); setSurvol(true); }}
      onDragLeave={() => setSurvol(false)}
      onDrop={(e) => { e.preventDefault(); setSurvol(false); if (e.dataTransfer.files[0]) choisir(e.dataTransfer.files[0]); }}
    >
      <IconUpload size={30} />
      <span className="drop-title">Déposez un fichier CSV ou Excel, ou cliquez pour le choisir</span>
      <span className="drop-hint">CSV · XLSX · XLS — {MAX_MO} Mo au plus</span>
      <input ref={input} type="file" accept=".csv,.xlsx,.xls" className="hidden"
             onChange={(e) => e.target.files?.[0] && choisir(e.target.files[0])} />
    </div>
  );
}

/* ── Étape 3 : le contrat proposé ─────────────────────────────────── */

function ContratPropose({ p }: { p: Proposition }) {
  const cle = new Set(p.contrat.cle_primaire || []);
  return (
    <>
      <p className="meta" style={{ marginBottom: '1rem' }}>
        {nombre(p.dataset.rows)} lignes<span className="meta-sep">·</span>
        {p.dataset.columns} colonnes<span className="meta-sep">·</span>
        imputation max {pct(p.contrat.imputation_max)} par colonne<span className="meta-sep">·</span>
        lignes perdues max {pct(p.contrat.lignes_perdues_max)}
      </p>
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr><th>Colonne</th><th>Type exigé</th><th>Vides tolérés</th><th>Contraintes</th></tr>
          </thead>
          <tbody>
            {p.contrat.colonnes.map((c) => (
              <tr key={c.nom}>
                <td className="mono">{c.nom}</td>
                <td><span className="tag">{c.type}</span></td>
                <td>{c.nullable ? `jusqu'à ${pct(c.taux_nul_max)}` : 'aucun'}</td>
                <td className="flex flex-wrap gap-1.5">
                  {cle.has(c.nom) && <span className="tag tag--key">clé primaire</span>}
                  {c.unique && !cle.has(c.nom) && <span className="tag">unique</span>}
                  {c.forme && <span className="tag">forme : {c.forme}</span>}
                  {c.domaine && <span className="tag">{c.domaine.length} valeurs permises</span>}
                  {(c.minimum !== null || c.maximum !== null) && <span className="tag">plage {c.minimum ?? '…'} – {c.maximum ?? '…'}</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {p.arbitrages.length > 0 && (
        <>
          <h3 className="h2" style={{ fontSize: '0.98rem', marginTop: '1.6rem' }}>
            Ce que le fichier ne peut pas tenir, et ce qui reste à décider
          </h3>
          <div className="notes">
            {p.arbitrages.filter((a) => !estInformation(a)).map((a, i) => (
              <p key={i} className="note-item"><IconAlert size={16} /><AvecColonnes texte={a} /></p>
            ))}
            {p.arbitrages.filter(estInformation).map((a, i) => (
              <p key={`i${i}`} className="note-item note-item--info"><IconShield size={16} /><AvecColonnes texte={a} /></p>
            ))}
          </div>
        </>
      )}
    </>
  );
}

/* ── Étape 4 : le rapport ─────────────────────────────────────────── */

function Colonne({ titre, ton, items, vide, Icone }: {
  titre: string; ton: 'ok' | 'bad' | 'hold'; items: string[]; vide: string;
  Icone: (p: { size?: number }) => React.ReactElement;
}) {
  return (
    <section className="ledger-col" aria-label={titre}>
      <header className={`ledger-head ledger-head--${ton}`}>
        <span className="flex items-center gap-2"><Icone size={16} />{titre}</span>
        <span className="ledger-count">{items.length}</span>
      </header>
      {items.length === 0 ? <p className="ledger-empty">{vide}</p> : (
        <ul className="ledger-list">
          {items.map((t, i) => (
            <li key={i} className="ledger-item">
              <span style={{ color: `var(--${ton === 'bad' ? 'danger' : ton})` }}><Icone size={14} /></span>
              <LigneJournal texte={t} />
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function Rapport({ r, fichier }: { r: PassageContrat; fichier: File }) {
  const base = fichier.name.replace(/\.[^.]+$/, '');
  const refuses = r.enonce['refuse_exprès'] || [];
  return (
    <>
      <div className={`verdict ${r.contrat_tenu ? 'verdict--ok' : 'verdict--bad'}`} role="status">
        <span className={r.contrat_tenu ? 'verdict-ok' : 'verdict-bad'}>
          {r.contrat_tenu ? <IconCheck size={28} /> : <IconX size={28} />}
        </span>
        <div className="flex-1 min-w-[16rem]">
          <p className="verdict-title">
            {r.contrat_tenu ? 'Le fichier peut entrer : le contrat est tenu.' : "Le fichier ne peut pas entrer tel quel : le contrat n'est pas tenu."}
          </p>
          <p className="lede" style={{ marginTop: '0.3rem', maxWidth: 'none' }}>
            Vérifié directement sur les données réparées, sans consulter le nettoyeur.
            {refuses.length > 0 && ` ${refuses.length} réparation(s) refusée(s) pour ne pas inventer de valeurs.`}
          </p>
        </div>
      </div>

      <div className="kpis">
        <div className="kpi">
          <p className="kpi-label">Score</p>
          <p className="kpi-value">{r.score_avant} → {r.score_apres}</p>
          <p className="kpi-sub">sur 100, plafonné à 55 si bloquant</p>
        </div>
        <div className="kpi">
          <p className="kpi-label">Violations</p>
          <p className="kpi-value">{r.violations_avant} → {r.violations_apres}</p>
          <p className="kpi-sub">avant → après</p>
        </div>
        <div className="kpi">
          <p className="kpi-label">Lignes</p>
          <p className="kpi-value">{nombre(r.lignes[1])}</p>
          <p className="kpi-sub">sur {nombre(r.lignes[0])} au départ</p>
        </div>
        <div className="kpi">
          <p className="kpi-label">Réparations</p>
          <p className="kpi-value">{r.journal.faits.length}</p>
          <p className="kpi-sub">chacune journalisée</p>
        </div>
      </div>

      <div className="ledger">
        <Colonne titre="Garanti" ton="ok" items={r.enonce.garanti} Icone={IconCheck}
                 vide="Aucune garantie vérifiée." />
        <Colonne titre="Non garanti" ton="bad" items={r.enonce.non_garanti} Icone={IconX}
                 vide="Aucune violation : tout ce que le contrat exige est tenu." />
        <Colonne titre="Refusé exprès" ton="hold" items={refuses} Icone={IconRefuse}
                 vide="Rien n'a dû être refusé." />
      </div>

      <details className="card" style={{ marginTop: '1.25rem' }}>
        <summary className="cursor-pointer text-[0.9rem] font-medium">
          Journal des réparations effectuées ({r.journal.faits.length})
        </summary>
        <ul style={{ marginTop: '0.8rem' }}>
          {r.enonce.fait.map((f, i) => (
            <li key={i} className="ledger-item" style={{ paddingLeft: 0 }}><LigneJournal texte={f} /></li>
          ))}
        </ul>
      </details>

      <div className="downloads">
        <button className="btn btn--primary" onClick={() => telecharger(r.csv_base64, `${base}_conforme.csv`, 'text/csv', true)}>
          <IconDownload size={16} /> Données réparées (CSV)
        </button>
        <button className="btn" onClick={() => telecharger(r.parquet_base64, `${base}_conforme.parquet`, 'application/octet-stream', true)}>
          <IconDownload size={16} /> Parquet typé
        </button>
        <button className="btn" onClick={() => telecharger(r.script, `contrat_${base}.py`, 'text/x-python')}>
          <IconDownload size={16} /> Script de rejeu (.py)
        </button>
        <button className="btn" onClick={() => telecharger(JSON.stringify(r.contrat, null, 2), `contrat_${base}.json`, 'application/json')}>
          <IconDownload size={16} /> Contrat (JSON)
        </button>
      </div>
      <p className="meta" style={{ marginTop: '0.6rem' }}>
        Le script de rejeu s&apos;exécute depuis le dépôt : <code>PYTHONPATH=backend python contrat_{base}.py {fichier.name}</code>
      </p>
    </>
  );
}

/* ── En option : la chaîne de préparation pour un modèle ─────────── */

function Preparation({ fichier }: { fichier: File }) {
  const [plan, setPlan] = useState<PlanPreparation | null>(null);
  const [cible, setCible] = useState('');
  const [charge, setCharge] = useState(false);
  const [erreur, setErreur] = useState<string | null>(null);

  const lancer = async () => {
    setCharge(true); setErreur(null);
    try { setPlan(await preparerChaine(fichier, cible.trim() || undefined)); }
    catch (e: any) { setErreur(e.message); }
    finally { setCharge(false); }
  };

  return (
    <div className="card">
      <p className="lede" style={{ maxWidth: '44rem' }}>
        Encodage et mise à l&apos;échelle ne sont jamais appliqués au fichier : ce serait faire fuiter
        le jeu de test dans l&apos;entraînement. L&apos;outil rend la chaîne, ajustée après la séparation.
      </p>
      <div className="flex flex-wrap items-end gap-3" style={{ marginTop: '1rem' }}>
        <label className="flex flex-col gap-1">
          <span className="section-label">Colonne cible (facultatif)</span>
          <input value={cible} onChange={(e) => setCible(e.target.value)} placeholder="ex. survived"
                 className="btn" style={{ fontFamily: 'var(--font-mono)', cursor: 'text', minWidth: '14rem' }} />
        </label>
        <button className="btn btn--primary" onClick={lancer} disabled={charge}>
          {charge && <span className="spinner" />} Construire la chaîne
        </button>
      </div>
      {erreur && <p className="alert alert--bad" style={{ marginTop: '1rem' }}>{erreur}</p>}
      {plan && (
        <div style={{ marginTop: '1.25rem' }}>
          <p className="meta">{plan.message}</p>
          <div className="table-wrap" style={{ marginTop: '0.8rem' }}>
            <table className="data-table">
              <thead><tr><th>Colonne</th><th>Traitement</th><th>Raison</th></tr></thead>
              <tbody>
                {plan.plan.numeriques.map((c) => (
                  <tr key={c}><td className="mono">{c}</td><td><span className="tag">numérique · mise à l&apos;échelle</span></td><td>—</td></tr>))}
                {plan.plan.categorielles.map((c) => (
                  <tr key={c}><td className="mono">{c}</td><td><span className="tag">catégorielle · one-hot</span></td><td>—</td></tr>))}
                {plan.plan.ecartees.map((c) => (
                  <tr key={c.nom}><td className="mono">{c.nom}</td>
                    <td><span className="tag" style={{ color: 'var(--hold)', borderColor: 'var(--hold)' }}>écartée</span></td>
                    <td>{c.raison}</td></tr>))}
              </tbody>
            </table>
          </div>
          <div className="downloads">
            <button className="btn btn--primary" onClick={() => telecharger(plan.script, plan.filename, 'text/x-python')}>
              <IconDownload size={16} /> Télécharger la chaîne ({plan.filename})
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── L'ensemble ───────────────────────────────────────────────────── */

export function Gate() {
  const [fichier, setFichier] = useState<File | null>(null);
  const [usage, setUsage] = useState<UsageKey>('base');
  const [proposition, setProposition] = useState<Proposition | null>(null);
  const [passage, setPassage] = useState<PassageContrat | null>(null);
  const [charge, setCharge] = useState<'' | 'propose' | 'passe'>('');
  const [erreur, setErreur] = useState<string | null>(null);

  const reinitialiser = (f: File | null) => { setFichier(f); setProposition(null); setPassage(null); setErreur(null); };
  const changerUsage = (u: UsageKey) => { setUsage(u); setProposition(null); setPassage(null); };

  const proposer = async () => {
    if (!fichier) return;
    setCharge('propose'); setErreur(null); setPassage(null);
    try { setProposition(await proposerContrat(fichier, usage)); }
    catch (e: any) { setErreur(e.message); }
    finally { setCharge(''); }
  };

  const passer = async (contrat?: Contrat) => {
    if (!fichier) return;
    setCharge('passe'); setErreur(null);
    try { setPassage(await passerContrat(fichier, usage, contrat)); }
    catch (e: any) { setErreur(e.message); }
    finally { setCharge(''); }
  };

  return (
    <div>
      <section className="step" aria-labelledby="etape-1">
        <div className="section-head"><h2 id="etape-1" className="h2"><span className="step-num">1</span>Le fichier</h2></div>
        <Depot fichier={fichier} onFichier={reinitialiser} onErreur={setErreur} />
      </section>

      <section className="step" aria-labelledby="etape-2">
        <div className="section-head">
          <h2 id="etape-2" className="h2"><span className="step-num">2</span>Ce que vous allez en faire</h2>
          <span className="section-label">l&apos;usage fixe ce qui est exigé</span>
        </div>
        <div className="usages" role="radiogroup" aria-label="Usage des données">
          {USAGES.map(({ cle, nom, regle, Icone }) => (
            <button key={cle} role="radio" aria-checked={usage === cle}
                    className={`usage ${usage === cle ? 'usage--on' : ''}`} onClick={() => changerUsage(cle)}>
              <Icone size={22} />
              <span className="usage-name">{nom}</span>
              <span className="usage-rule">{regle}</span>
            </button>
          ))}
        </div>
        <div style={{ marginTop: '1.1rem' }}>
          <button className="btn btn--primary" onClick={proposer} disabled={!fichier || charge !== ''}>
            {charge === 'propose' && <span className="spinner" />}
            Proposer le contrat
          </button>
          {!fichier && <span className="meta" style={{ marginLeft: '0.9rem' }}>déposez d&apos;abord un fichier</span>}
        </div>
      </section>

      {erreur && <p className="alert alert--bad" role="alert" style={{ marginBottom: '1.5rem' }}>{erreur}</p>}

      {proposition && fichier && (
        <section className="step" aria-labelledby="etape-3">
          <div className="section-head">
            <h2 id="etape-3" className="h2"><span className="step-num">3</span>Le contrat proposé</h2>
            <span className="section-label">rien n&apos;a encore été modifié</span>
          </div>
          <div className="card">
            <ContratPropose p={proposition} />
            <div className="downloads">
              <button className="btn btn--primary" onClick={() => passer(proposition.contrat)} disabled={charge !== ''}>
                {charge === 'passe' && <span className="spinner" />}
                Réparer et vérifier
              </button>
            </div>
          </div>
        </section>
      )}

      {passage && fichier && (
        <section className="step" aria-labelledby="etape-4">
          <div className="section-head">
            <h2 id="etape-4" className="h2"><span className="step-num">4</span>Le rapport</h2>
            <span className="section-label">usage : {USAGES.find((u) => u.cle === passage.usage)?.nom}</span>
          </div>
          <Rapport r={passage} fichier={fichier} />
        </section>
      )}

      {passage && fichier && (
        <section className="step" aria-labelledby="etape-5">
          <div className="section-head">
            <h2 id="etape-5" className="h2"><span className="step-num">5</span>Pour un modèle : la chaîne de préparation</h2>
            <span className="section-label">facultatif</span>
          </div>
          <Preparation fichier={fichier} />
        </section>
      )}
    </div>
  );
}
