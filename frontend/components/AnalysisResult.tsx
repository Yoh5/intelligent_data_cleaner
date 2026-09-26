"use client";

import React, { useMemo, useState, useEffect } from "react";
import { getSuggestionsBatch, generateCode, type BatchStrategyItem, type CleaningStrategy } from "@/lib/api";
import { IssueCard } from "./IssueCard";
import { SuggestionModal } from "./SuggestionModal";
import { CodeExport } from "./CodeExport";

interface ColumnInfo {
  name: string;
  dtype: string;
  missing_count: number;
  missing_rate: number;
  unique_count: number;
  sample_values: any[];
  semantic_type?: string;
}

interface IssueInfo {
  column: string | null;
  issue: string;
  type?: string;
  severity: "high" | "medium" | "low" | "critical";
  count?: number;
  rate?: number;
  semantic_type?: string;
  description?: string;
  affected_rows?: number;
}

interface AnalysisResultProps {
  analysis: {
    shape: [number, number];
    columns: Record<string, ColumnInfo>;
    issues: IssueInfo[];
    dataset_info?: { filename: string };
    raw_profile?: {
      sample_rows?: Array<Record<string, any>>;
      dtypes?: Record<string, string>;
      total_missing?: number;
    };
  };
  originalData: Array<Record<string, any>>;
  originalFile?: File | null;
  onGenerateCode?: (code: string) => void;
}

export const AnalysisResult: React.FC<AnalysisResultProps> = ({
  analysis,
  originalFile,
  onGenerateCode,
}) => {
  const [suggestionResults, setSuggestionResults] = useState<BatchStrategyItem[]>([]);
  const [loadingSuggestions, setLoadingSuggestions] = useState(true);
  const [activeModal, setActiveModal] = useState<number | null>(null);
  const [selectedStrategies, setSelectedStrategies] = useState<Record<number, CleaningStrategy>>({});
  const [isGenerating, setIsGenerating] = useState(false);
  const [generatedScript, setGeneratedScript] = useState<string | null>(null);
  const [scriptFilename, setScriptFilename] = useState("clean_dataset.py");
  const [error, setError] = useState<string | null>(null);
  const [showPreview, setShowPreview] = useState(false);

  // Stable key: only re-fetch suggestions when the file or issue count changes
  const analysisKey = `${analysis.dataset_info?.filename ?? "unknown"}-${analysis.issues.length}`;

  useEffect(() => {
    const fetchSuggestions = async () => {
      setLoadingSuggestions(true);
      if (analysis.issues.length === 0) {
        setLoadingSuggestions(false);
        return;
      }
      try {
        const colTypes = Object.fromEntries(
          Object.entries(analysis.columns).map(([k, v]) => [k, v.semantic_type || v.dtype])
        );
        const result = await getSuggestionsBatch({
          dataset_name: analysis.dataset_info?.filename || "dataset",
          column_types: colTypes,
          issues: analysis.issues as any,
          sample_data: [],
        });
        setSuggestionResults(result.results);
        // Auto-select recommended strategy per issue
        const autoSelected: Record<number, CleaningStrategy> = {};
        result.results.forEach((r, idx) => {
          if (r.strategies.length > 0) {
            autoSelected[idx] = r.strategies[r.recommended ?? 0];
          }
        });
        setSelectedStrategies(autoSelected);
      } catch {
        // Fail silently — user can still generate with defaults
      } finally {
        setLoadingSuggestions(false);
      }
    };
    fetchSuggestions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [analysisKey]);

  const handleStrategySelect = (issueIdx: number, strategy: CleaningStrategy) => {
    setSelectedStrategies((prev) => ({ ...prev, [issueIdx]: strategy }));
    setActiveModal(null);
  };

  const handleSelectAll = () => {
    const all: Record<number, CleaningStrategy> = {};
    suggestionResults.forEach((r, idx) => {
      if (r.strategies.length > 0) {
        all[idx] = r.strategies[r.recommended ?? 0];
      }
    });
    setSelectedStrategies(all);
  };

  const handleGenerateCode = async () => {
    const steps = Object.entries(selectedStrategies).map(([idxStr, strategy]) => {
      const issue = analysis.issues[Number(idxStr)];
      return {
        column: issue.column,
        issue_type: issue.issue || issue.type || "unknown",
        strategy_name: strategy.name,
        code: strategy.code_preview,
      };
    });

    if (steps.length === 0) {
      setError("Cliquez sur un problème pour choisir une stratégie de nettoyage");
      return;
    }

    setIsGenerating(true);
    setError(null);
    try {
      const response = await generateCode({
        dataset_name: analysis.dataset_info?.filename || "dataset.csv",
        steps,
      });
      setGeneratedScript(response.script);
      setScriptFilename(
        response.filename || `clean_${analysis.dataset_info?.filename?.split(".")[0]}.py`
      );
      onGenerateCode?.(response.script);
    } catch (err: any) {
      setError(err.message || "Erreur lors de la génération du script");
    } finally {
      setIsGenerating(false);
    }
  };

  const resetScript = () => {
    setGeneratedScript(null);
    setScriptFilename("clean_dataset.py");
  };

  const codeSteps = useMemo(
    () =>
      Object.entries(selectedStrategies).map(([idxStr, strategy]) => ({
        strategy_name: strategy.name,
        column: analysis.issues[Number(idxStr)]?.column ?? undefined,
        code: strategy.code_preview,
      })),
    [selectedStrategies, analysis.issues]
  );

  const selectedCount = Object.keys(selectedStrategies).length;
  const allSelected =
    !loadingSuggestions &&
    analysis.issues.length > 0 &&
    selectedCount === analysis.issues.length;

  const sampleRows = analysis.raw_profile?.sample_rows;
  const sampleColumns = sampleRows && sampleRows.length > 0 ? Object.keys(sampleRows[0]) : [];

  return (
    <div>
      {/* Ce que le fichier est, en une ligne de mesures. Quatre grands
          chiffres dans quatre tuiles disaient surtout « ceci est un
          tableau de bord » ; la question du lecteur est « combien de
          colonnes ne vont pas », et elle tient sur une ligne. */}
      <div className="panel">
        <p className="meta">
          {analysis.shape[0].toLocaleString('fr-FR')} lignes
          <span className="meta-sep">·</span>
          {analysis.shape[1]} colonnes
          <span className="meta-sep">·</span>
          {analysis.issues.length === 0
            ? 'aucun problème détecté'
            : `${analysis.issues.length} à problème, dont ${analysis.issues.filter(
                (i) => i.severity === 'critical' || i.severity === 'high',
              ).length} sérieux`}
        </p>

        {sampleRows && sampleRows.length > 0 && (
          <button onClick={() => setShowPreview((v) => !v)} className="btn btn--quiet mt-3">
            {showPreview ? 'masquer les données' : 'voir les 5 premières lignes'}
          </button>
        )}
      </div>

      {showPreview && sampleRows && sampleColumns.length > 0 && (
        <div className="panel overflow-x-auto">
          <table className="sheet-table">
            <thead>
              <tr>
                {sampleColumns.map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sampleRows.slice(0, 5).map((row: any, i: number) => (
                <tr key={i}>
                  {sampleColumns.map((col) => (
                    <td key={col}>
                      {row[col] === null || row[col] === '' ? (
                        <span className="cell-empty">vide</span>
                      ) : (
                        String(row[col])
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Les colonnes à problème, dans l'ordre de gravité. */}
      <div className="panel">
        <div className="section-head">
          <span className="section-label">ce qui ne va pas</span>
          <span className="flex items-baseline gap-4">
            {!loadingSuggestions && !allSelected && analysis.issues.length > 0 && (
              <button onClick={handleSelectAll} className="btn btn--quiet">
                tout accepter
              </button>
            )}
            <span className="section-label">
              {loadingSuggestions
                ? 'lecture des stratégies…'
                : `${selectedCount}/${analysis.issues.length} décidés`}
            </span>
          </span>
        </div>

        {analysis.issues.length === 0 ? (
          <p className="alert">
            Rien à corriger ici. Le fichier peut être utilisé tel quel.
          </p>
        ) : (
          analysis.issues.map((issue, idx) => (
            <div key={idx}>
              <IssueCard
                issue={issue as any}
                index={idx}
                onClick={() => !loadingSuggestions && setActiveModal(idx)}
                isSelected={!!selectedStrategies[idx]}
              />
              {selectedStrategies[idx] && (
                <p className="meta" style={{ paddingLeft: '0.1rem', paddingBottom: '0.5rem' }}>
                  {selectedStrategies[idx].name}
                  <span className="meta-sep">·</span>
                  <button onClick={() => setActiveModal(idx)} className="btn btn--quiet">
                    changer
                  </button>
                </p>
              )}
              {suggestionResults[idx]?.rationale && (
                <p className="note" style={{ marginBottom: '0.6rem' }}>
                  <span className="note-who">raison du choix</span>
                  {suggestionResults[idx].rationale}
                </p>
              )}
            </div>
          ))
        )}
      </div>

      {error && <p className="alert alert--bad panel">{error}</p>}

      {!generatedScript && (
        <button
          onClick={handleGenerateCode}
          disabled={isGenerating || selectedCount === 0}
          className="btn btn--primary"
        >
          {isGenerating
            ? 'génération…'
            : `écrire le script (${selectedCount} décision${selectedCount > 1 ? 's' : ''})`}
        </button>
      )}

      {/* Generated script */}
      {generatedScript && (
        <CodeExport
          steps={codeSteps}
          generatedScript={generatedScript}
          filename={scriptFilename}
          originalFile={originalFile}
          onRegenerate={resetScript}
        />
      )}

      {/* Strategy modal */}
      {activeModal !== null && (
        <SuggestionModal
          issue={{
            type: analysis.issues[activeModal]?.type || analysis.issues[activeModal]?.issue || "",
            column: analysis.issues[activeModal]?.column || null,
            severity: analysis.issues[activeModal]?.severity || "medium",
          }}
          strategies={suggestionResults[activeModal]?.strategies || []}
          recommended={suggestionResults[activeModal]?.recommended ?? 0}
          rationale={suggestionResults[activeModal]?.rationale}
          onSelect={(strategy) => handleStrategySelect(activeModal, strategy)}
          onClose={() => setActiveModal(null)}
        />
      )}
    </div>
  );
};

export default AnalysisResult;
