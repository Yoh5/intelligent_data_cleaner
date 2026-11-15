'use client';

/* « Ces deux fichiers sont-ils joignables ? »

   Une jointure ne plante jamais : elle perd des lignes ou les multiplie, en
   silence. On dépose les deux fichiers, on désigne la clé, et on lit ce que
   la jointure FERAIT — sans la faire, et sans rien réparer. */

import React, { useState } from 'react';
import { DiagnosticJointure, colonnesDuFichier, diagnostiquerJointure } from '@/lib/api';
import { Depot } from './Gate';
import { IconAlert, IconCheck, IconX } from './Illustrations';

type Cote = { fichier: File | null; colonnes: string[]; lignes: number };
const VIDE: Cote = { fichier: null, colonnes: [], lignes: 0 };
const nombre = (n: number) => n.toLocaleString('fr-FR');

const CARDINALITES: Record<string, string> = {
  '1:1': 'une ligne à gauche, une à droite',
  'n:1': "plusieurs lignes à gauche pour une à droite, le cas normal d'une table de référence",
  '1:n': 'une ligne à gauche en trouve plusieurs à droite : la gauche sera répétée',
  'n:m': 'clé répétée des deux côtés : les lignes se multiplient',
};

export function Jointure() {
  const [gauche, setGauche] = useState<Cote>(VIDE);
  const [droite, setDroite] = useState<Cote>(VIDE);
  const [paires, setPaires] = useState<[string, string][]>([['', '']]);
  const [seuil, setSeuil] = useState(100);
  const [diag, setDiag] = useState<DiagnosticJointure | null>(null);
  const [charge, setCharge] = useState(false);
  const [erreur, setErreur] = useState<string | null>(null);

  const deposer = (poser: (c: Cote) => void, autre: Cote) => async (f: File | null) => {
    setDiag(null); setErreur(null);
    if (!f) { poser(VIDE); setPaires([['', '']]); return; }
    try {
      const { colonnes, lignes } = await colonnesDuFichier(f);
      poser({ fichier: f, colonnes, lignes });
      // Proposer la première colonne commune : un point de départ, pas une décision.
      const commune = colonnes.find((c) => autre.colonnes.includes(c));
      setPaires([[commune ?? '', commune ?? '']]);
    } catch (e: any) { poser(VIDE); setErreur(e.message); }
  };

  const changer = (i: number, cote: 0 | 1, v: string) => {
    setDiag(null);
    setPaires((p) => p.map((x, k) => (k === i ? (cote === 0 ? [v, x[1]] : [x[0], v]) : x)) as [string, string][]);
  };

  const pret = gauche.fichier && droite.fichier && paires.every(([a, b]) => a && b);

  const lancer = async () => {
    if (!gauche.fichier || !droite.fichier) return;
    setCharge(true); setErreur(null);
    try { setDiag(await diagnostiquerJointure(gauche.fichier, droite.fichier, paires, seuil / 100)); }
    catch (e: any) { setErreur(e.message); }
    finally { setCharge(false); }
  };

  return (
    <div>
      <section className="step" aria-labelledby="j-1">
        <div className="section-head">
          <h2 id="j-1" className="h2"><span className="step-num">1</span>Les deux fichiers</h2>
          <span className="section-label">rien ne sera joint ni modifié</span>
        </div>
        <div className="pair">
          <div>
            <p className="section-label" style={{ marginBottom: '0.5rem' }}>Gauche : celui dont on veut garder toutes les lignes</p>
            <Depot fichier={gauche.fichier} onFichier={deposer(setGauche, droite)} onErreur={setErreur} />
            {gauche.fichier && <p className="meta" style={{ marginTop: '0.4rem' }}>{nombre(gauche.lignes)} lignes · {gauche.colonnes.length} colonnes</p>}
          </div>
          <div>
            <p className="section-label" style={{ marginBottom: '0.5rem' }}>Droite : celui où l&apos;on va chercher</p>
            <Depot fichier={droite.fichier} onFichier={deposer(setDroite, gauche)} onErreur={setErreur} />
            {droite.fichier && <p className="meta" style={{ marginTop: '0.4rem' }}>{nombre(droite.lignes)} lignes · {droite.colonnes.length} colonnes</p>}
          </div>
        </div>
      </section>

      {gauche.fichier && droite.fichier && (
        <section className="step" aria-labelledby="j-2">
          <div className="section-head">
            <h2 id="j-2" className="h2"><span className="step-num">2</span>La clé</h2>
            <span className="section-label">plusieurs colonnes pour une clé composite</span>
          </div>
          <div className="card">
            {paires.map(([a, b], i) => (
              <div key={i} className="keyrow">
                <label className="flex flex-col gap-1">
                  <span className="section-label">colonne de gauche</span>
                  <select className="select" value={a} onChange={(e) => changer(i, 0, e.target.value)}>
                    <option value="">choisir…</option>
                    {gauche.colonnes.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </label>
                <span className="keyrow-eq" aria-hidden="true">=</span>
                <label className="flex flex-col gap-1">
                  <span className="section-label">colonne de droite</span>
                  <select className="select" value={b} onChange={(e) => changer(i, 1, e.target.value)}>
                    <option value="">choisir…</option>
                    {droite.colonnes.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </label>
                {paires.length > 1 && (
                  <button className="btn btn--quiet btn--drop" aria-label="Retirer cette colonne de la clé"
                          onClick={() => { setDiag(null); setPaires((p) => p.filter((_, k) => k !== i)); }}>
                    <IconX size={16} /> retirer
                  </button>
                )}
              </div>
            ))}
            <button className="btn btn--quiet" onClick={() => { setDiag(null); setPaires((p) => [...p, ['', '']]); }}>
              + ajouter une colonne à la clé
            </button>

            <div className="flex flex-wrap items-end gap-4" style={{ marginTop: '1.1rem' }}>
              <label className="flex flex-col gap-1">
                <span className="section-label">part des lignes de gauche qui doivent trouver une ligne</span>
                <span className="flex items-center gap-2">
                  <input type="number" min={0} max={100} step={1} value={seuil} className="select" style={{ minWidth: 0, width: '6rem' }}
                         onChange={(e) => { setDiag(null); setSeuil(Math.max(0, Math.min(100, Number(e.target.value)))); }} />
                  <span className="meta">%</span>
                </span>
              </label>
              <button className="btn btn--primary" onClick={lancer} disabled={!pret || charge}>
                {charge && <span className="spinner" />} Diagnostiquer la jointure
              </button>
            </div>
          </div>
        </section>
      )}

      {erreur && <p className="alert alert--bad" role="alert" style={{ marginBottom: '1.5rem' }}>{erreur}</p>}

      {diag && (
        <section className="step" aria-labelledby="j-3">
          <div className="section-head">
            <h2 id="j-3" className="h2"><span className="step-num">3</span>Ce que la jointure ferait</h2>
            <span className="section-label">{diag.cle_gauche.join(' + ')} = {diag.cle_droite.join(' + ')}</span>
          </div>

          <div className={`verdict ${diag.joignable ? 'verdict--ok' : 'verdict--bad'}`} role="status">
            <span className={diag.joignable ? 'verdict-ok' : 'verdict-bad'}>
              {diag.joignable ? <IconCheck size={28} /> : <IconX size={28} />}
            </span>
            <div className="flex-1 min-w-[16rem]">
              <p className="verdict-title">
                {!diag.joignable ? "Non joignables en l'état."
                  : diag.orphelines === 0 ? 'Joignables : aucune ligne perdue ni multipliée.'
                  : `Joignables dans la tolérance fixée, mais ${nombre(diag.orphelines)} ligne(s) de gauche resteront sans correspondance.`}
              </p>
              <p className="lede" style={{ marginTop: '0.3rem', maxWidth: 'none' }}>
                Cardinalité {diag.cardinalite} : {CARDINALITES[diag.cardinalite]}.
              </p>
            </div>
          </div>

          <div className="kpis">
            <div className="kpi">
              <p className="kpi-label">Couverture</p>
              <p className="kpi-value">{(diag.couverture * 100).toFixed(1).replace('.', ',')} %</p>
              <p className="kpi-sub">exigée : {Math.round(diag.couverture_min * 100)} %</p>
            </div>
            <div className="kpi">
              <p className="kpi-label">Sans correspondance</p>
              <p className="kpi-value">{nombre(diag.orphelines)}</p>
              <p className="kpi-sub">sur {nombre(diag.lignes_gauche)} lignes à gauche</p>
            </div>
            <div className="kpi">
              <p className="kpi-label">Presque-correspondances</p>
              <p className="kpi-value">{nombre(diag.presque)}</p>
              <p className="kpi-sub">espace, casse, zéro initial</p>
            </div>
            <div className="kpi">
              <p className="kpi-label">Lignes du résultat</p>
              <p className="kpi-value">{nombre(diag.lignes_resultat)}</p>
              <p className="kpi-sub">en jointure interne</p>
            </div>
          </div>

          {diag.problemes.length > 0 && (
            <div className="notes">
              {diag.problemes.map((p) => (
                <p key={p.code} className="note-item note-item--bad"><IconX size={16} /><span>{p.message}</span></p>
              ))}
            </div>
          )}

          {diag.exemples_presque.length > 0 && (
            <div className="card" style={{ marginTop: '1.25rem' }}>
              <h3 className="h2" style={{ fontSize: '0.98rem' }}>Des clés qui se ressemblent sans s&apos;égaler</h3>
              <p className="lede" style={{ marginTop: '0.3rem', maxWidth: '46rem' }}>
                Rien n&apos;est corrigé : décider que deux écritures désignent la même clé est votre décision, pas un nettoyage.
              </p>
              <div className="table-wrap" style={{ marginTop: '0.8rem' }}>
                <table className="data-table">
                  <thead><tr><th>À gauche</th><th>À droite</th></tr></thead>
                  <tbody>
                    {diag.exemples_presque.map(([a, b], i) => (
                      <tr key={i}><td><span className="cle">{a}</span></td><td><span className="cle">{b}</span></td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div className="notes">
            {diag.exemples_orphelines.length > 0 && (
              <p className="note-item"><IconAlert size={16} /><span>
                Clés de gauche absentes à droite, par exemple : {diag.exemples_orphelines.map((k, i) => (
                  <span key={i}>{i > 0 && ', '}<span className="cle">{k}</span></span>))}
              </span></p>
            )}
            {(diag.cles_vides_gauche > 0 || diag.cles_vides_droite > 0) && (
              <p className="note-item"><IconAlert size={16} /><span>
                Clés vides : {nombre(diag.cles_vides_gauche)} à gauche, {nombre(diag.cles_vides_droite)} à droite. Une clé vide ne trouve jamais rien.
              </span></p>
            )}
            {diag.cles_droite_inutilisees > 0 && (
              <p className="note-item note-item--info"><IconCheck size={16} /><span>
                {nombre(diag.cles_droite_inutilisees)} clé(s) de droite ne sont jamais appelées par la gauche : une information, pas un défaut.
              </span></p>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
