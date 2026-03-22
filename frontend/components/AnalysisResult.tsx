"use client";

import React, { useState } from "react";
import { getSuggestionsBatch, generateCode } from "@/lib/api";

interface AnalysisResultProps {
  analysis: {
    shape: [number, number];
    columns: Record<string, {
      name: string;
      dtype: string;
      missing_count: number;
      missing_rate: number;
      unique_count: number;
      sample_values: any[];
      semantic_type?: string;
    }>;
    issues: Array<{
      column: string;
      issue: string;
      type?: string;
      severity: "high" | "medium" | "low" | "critical";
      count?: number;
      rate?: number;
      semantic_type?: string;
      description?: string;
      affected_rows?: number;
    }>;
    dataset_info?: {
      filename: string;
    };
  };
  originalData: Array<Record<string, any>>;
  onGenerateCode: (code: string) => void;
}

export const AnalysisResult: React.FC<AnalysisResultProps> = ({
  analysis,
  originalData,
  onGenerateCode,
}) => {
  const [selectedSteps, setSelectedSteps] = useState<Set<string>>(new Set());
  const [isGenerating, setIsGenerating] = useState(false);
  const [preview, setPreview] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const toggleStep = (stepId: string) => {
    const newSteps = new Set(selectedSteps);
    if (newSteps.has(stepId)) {
      newSteps.delete(stepId);
    } else {
      newSteps.add(stepId);
    }
    setSelectedSteps(newSteps);
    setError(null);
  };

  const selectAll = () => {
    const allSteps = analysis.issues.map((issue, idx) => 
      `${issue.column}-${issue.issue}-${idx}`
    );
    setSelectedSteps(new Set(allSteps));
  };

  const deselectAll = () => {
    setSelectedSteps(new Set());
  };

  const handleGenerateCode = async () => {
    if (selectedSteps.size === 0) {
      setError("Veuillez sélectionner au moins un problème à résoudre");
      return;
    }

    setIsGenerating(true);
    setError(null);
    setPreview(null);

    try {
      // Préparer les étapes sélectionnées
      const selectedIssues = analysis.issues.filter((issue, idx) =>
        selectedSteps.has(`${issue.column}-${issue.issue}-${idx}`)
      );

      // Construire les étapes pour l'API
      const steps = selectedIssues.map((issue, idx) => {
        // Générer le code approprié selon le type de problème
        let code = "";
        const col = issue.column;

        if (issue.issue === "missing_values" || issue.issue === "missing") {
          code = `if '${col}' in df.columns:
    df['${col}'] = pd.to_numeric(df['${col}'], errors='coerce')
    df['${col}'] = df['${col}'].fillna(df['${col}'].median())`;
        } else if (issue.issue === "duplicate") {
          code = `df = df.drop_duplicates()`;
        } else if (issue.issue === "inconsistent" || issue.issue === "mixed_types") {
          code = `if '${col}' in df.columns:
    df['${col}'] = df['${col}'].astype(str).str.replace(',', '.')
    df['${col}'] = pd.to_numeric(df['${col}'], errors='coerce')`;
        } else {
          code = `# Nettoyage pour ${col}`;
        }

        return {
          column: issue.column,
          issue_type: issue.issue || issue.type || "unknown",
          strategy_name: issue.severity || "auto",
          code: code
        };
      });

      console.log("Envoi de la requête avec steps:", steps);

      const response = await generateCode({
        dataset_name: analysis.dataset_info?.filename || "dataset.csv",
        steps: steps,
      });

      if (response.script) {
        setPreview(response.script);
        onGenerateCode(response.script);
        console.log("Script généré avec succès!");
      } else {
        throw new Error("Pas de script dans la réponse");
      }
    } catch (err: any) {
      console.error("Erreur génération:", err);
      setError(err.message || "Erreur lors de la génération du script");
    } finally {
      setIsGenerating(false);
    }
  };

  const downloadCode = () => {
    if (!preview) return;
    const blob = new Blob([preview], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `clean_${analysis.dataset_info?.filename?.split(".")[0] || "dataset"}.py`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const copyCode = () => {
    if (!preview) return;
    navigator.clipboard.writeText(preview);
    alert("Code copié dans le presse-papiers!");
  };

  return (
    <div className="space-y-6">
      {/* Résumé */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl p-6 border border-blue-100">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">
          📊 Résumé de l'analyse
        </h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-blue-600">
              {analysis.shape[0].toLocaleString()}
            </div>
            <div className="text-sm text-gray-600">Lignes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-blue-600">
              {analysis.shape[1]}
            </div>
            <div className="text-sm text-gray-600">Colonnes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-orange-600">
              {analysis.issues.length}
            </div>
            <div className="text-sm text-gray-600">Problèmes</div>
          </div>
          <div className="bg-white rounded-lg p-3 shadow-sm">
            <div className="text-2xl font-bold text-red-600">
              {analysis.issues.filter((i) => i.severity === "critical" || i.severity === "high").length}
            </div>
            <div className="text-sm text-gray-600">Critiques</div>
          </div>
        </div>
      </div>

      {/* Liste des problèmes */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="p-4 bg-gray-50 border-b border-gray-200 flex justify-between items-center flex-wrap gap-2">
          <h3 className="font-semibold text-gray-800">🔍 Problèmes identifiés</h3>
          <div className="flex gap-2">
            <button
              onClick={selectAll}
              className="text-sm bg-blue-100 hover:bg-blue-200 text-blue-700 px-3 py-1 rounded-full transition-colors"
            >
              Tout sélectionner
            </button>
            <button
              onClick={deselectAll}
              className="text-sm bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-1 rounded-full transition-colors"
            >
              Tout désélectionner
            </button>
          </div>
        </div>

        <div className="divide-y divide-gray-100 max-h-[500px] overflow-y-auto">
          {analysis.issues.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              ✅ Aucun problème détecté dans ce dataset
            </div>
          ) : (
            analysis.issues.map((issue, idx) => {
              const stepId = `${issue.column}-${issue.issue}-${idx}`;
              const isSelected = selectedSteps.has(stepId);

              // Déterminer la couleur selon la sévérité
              const severityColors = {
                critical: "bg-red-50 border-red-200",
                high: "bg-orange-50 border-orange-200",
                medium: "bg-yellow-50 border-yellow-200",
                low: "bg-blue-50 border-blue-200"
              };

              const severityBadge = {
                critical: "bg-red-100 text-red-800",
                high: "bg-orange-100 text-orange-800",
                medium: "bg-yellow-100 text-yellow-800",
                low: "bg-blue-100 text-blue-800"
              };

              return (
                <div
                  key={stepId}
                  className={`p-4 cursor-pointer transition-all hover:bg-gray-50 border-l-4 ${
                    isSelected ? "border-blue-500 bg-blue-50/30" : "border-transparent"
                  } ${severityColors[issue.severity]}`}
                  onClick={() => toggleStep(stepId)}
                >
                  <div className="flex items-start gap-3">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleStep(stepId)}
                      className="mt-1 w-4 h-4 text-blue-600 rounded focus:ring-blue-500 cursor-pointer"
                      onClick={(e) => e.stopPropagation()}
                    />
                    <div className="flex-1">
                      <div className="flex justify-between items-start mb-1">
                        <h4 className="font-medium text-gray-800">
                          {issue.column ? `Colonne "${issue.column}"` : "Dataset entier"}
                        </h4>
                        <span className={`text-xs px-2 py-1 rounded-full font-medium ${severityBadge[issue.severity]}`}>
                          {issue.severity.toUpperCase()}
                        </span>
                      </div>
                      <p className="text-sm text-gray-600 mb-1">
                        {issue.description || `Problème: ${issue.issue}`}
                      </p>
                      {issue.count && (
                        <p className="text-xs text-gray-500">
                          {issue.count} occurrence{issue.count > 1 ? 's' : ''} 
                          {issue.rate && ` (${(issue.rate * 100).toFixed(1)}%)`}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* Erreur */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700">
          ❌ {error}
        </div>
      )}

      {/* Actions */}
      <div className="flex gap-3 flex-wrap">
        <button
          onClick={handleGenerateCode}
          disabled={isGenerating || selectedSteps.size === 0}
          className="flex-1 min-w-[200px] bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 
                   disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-lg
                   transition-all transform hover:scale-[1.02] active:scale-[0.98] shadow-lg"
        >
          {isGenerating ? (
            <span className="flex items-center justify-center gap-2">
              <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"/>
              </svg>
              Génération en cours...
            </span>
          ) : (
            `🚀 Générer le script (${selectedSteps.size} étape${selectedSteps.size > 1 ? "s" : ""})`
          )}
        </button>

        {preview && (
          <>
            <button
              onClick={downloadCode}
              className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-lg transition-all shadow-lg"
            >
              💾 Télécharger
            </button>
            <button
              onClick={copyCode}
              className="bg-gray-600 hover:bg-gray-700 text-white font-semibold py-3 px-6 rounded-lg transition-all shadow-lg"
            >
              📋 Copier
            </button>
          </>
        )}
      </div>

      {/* Aperçu du code */}
      {preview && (
        <div className="bg-gray-900 rounded-xl overflow-hidden shadow-2xl">
          <div className="bg-gray-800 px-4 py-3 flex justify-between items-center">
            <h3 className="text-gray-200 font-mono text-sm">📄 Script Python généré</h3>
            <span className="text-xs text-gray-400">{preview.length} caractères</span>
          </div>
          <pre className="p-4 overflow-x-auto text-sm text-gray-300 font-mono leading-relaxed max-h-[500px] overflow-y-auto">
            {preview}
          </pre>
        </div>
      )}
    </div>
  );
};

export default AnalysisResult;
