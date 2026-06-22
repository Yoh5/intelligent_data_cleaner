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
    <div className="space-y-6">
      {/* Summary */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl p-6 border border-blue-100">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Résumé de l'analyse</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-blue-600">{analysis.shape[0].toLocaleString()}</div>
            <div className="text-sm text-gray-600">Lignes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-blue-600">{analysis.shape[1]}</div>
            <div className="text-sm text-gray-600">Colonnes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-orange-600">{analysis.issues.length}</div>
            <div className="text-sm text-gray-600">Problèmes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-red-600">
              {analysis.issues.filter((i) => i.severity === "critical" || i.severity === "high").length}
            </div>
            <div className="text-sm text-gray-600">Critiques</div>
          </div>
        </div>

        {/* Data preview toggle */}
        {sampleRows && sampleRows.length > 0 && (
          <button
            onClick={() => setShowPreview((v) => !v)}
            className="mt-4 text-sm text-blue-600 hover:text-blue-800 font-medium"
          >
            {showPreview ? '▲ Masquer l\'aperçu' : '▼ Aperçu des données (5 premières lignes)'}
          </button>
        )}
      </div>

      {/* Data preview table */}
      {showPreview && sampleRows && sampleColumns.length > 0 && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="p-3 bg-gray-50 border-b border-gray-200">
            <span className="text-sm font-medium text-gray-700">Aperçu — {sampleColumns.length} colonnes</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  {sampleColumns.map((col) => (
                    <th key={col} className="px-3 py-2 text-left font-medium text-gray-600 whitespace-nowrap">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sampleRows.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                    {sampleColumns.map((col) => (
                      <td key={col} className="px-3 py-1.5 text-gray-700 whitespace-nowrap max-w-[200px] truncate">
                        {row[col] == null ? (
                          <span className="text-red-400 italic">null</span>
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
        </div>
      )}

      {/* Issues */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="p-4 bg-gray-50 border-b border-gray-200 flex justify-between items-center flex-wrap gap-2">
          <h3 className="font-semibold text-gray-800">Problèmes identifiés</h3>
          <div className="flex items-center gap-3">
            {!loadingSuggestions && !allSelected && analysis.issues.length > 0 && (
              <button
                onClick={handleSelectAll}
                className="text-xs bg-blue-50 hover:bg-blue-100 text-blue-700 px-3 py-1.5 rounded-lg font-medium transition-colors"
              >
                Tout sélectionner
              </button>
            )}
            {loadingSuggestions ? (
              <span className="text-sm text-gray-400 animate-pulse">Chargement des stratégies...</span>
            ) : (
              <span className="text-sm text-gray-500">
                {selectedCount}/{analysis.issues.length} stratégie{selectedCount > 1 ? "s" : ""} sélectionnée{selectedCount > 1 ? "s" : ""}
              </span>
            )}
          </div>
        </div>

        <div className="p-4 space-y-3">
          {analysis.issues.length === 0 ? (
            <div className="py-8 text-center text-gray-500">
              Aucun problème détecté dans ce dataset
            </div>
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
                  <div className="mt-1 ml-2 flex items-center gap-2 text-sm text-green-700">
                    <span className="font-medium">✓ {selectedStrategies[idx].name}</span>
                    <button
                      onClick={() => setActiveModal(idx)}
                      className="text-blue-500 hover:underline text-xs"
                    >
                      Changer
                    </button>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700">{error}</div>
      )}

      {/* Generate button */}
      {!generatedScript && (
        <button
          onClick={handleGenerateCode}
          disabled={isGenerating || selectedCount === 0}
          className="w-full bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700
                     disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-lg
                     transition-all transform hover:scale-[1.02] active:scale-[0.98] shadow-lg"
        >
          {isGenerating ? (
            <span className="flex items-center justify-center gap-2">
              <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
              Génération en cours...
            </span>
          ) : (
            `Générer le script (${selectedCount} stratégie${selectedCount > 1 ? "s" : ""})`
          )}
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
          onSelect={(strategy) => handleStrategySelect(activeModal, strategy)}
          onClose={() => setActiveModal(null)}
        />
      )}
    </div>
  );
};

export default AnalysisResult;
