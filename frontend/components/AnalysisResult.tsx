"use client";

import React, { useState, useCallback } from "react";
import { IssueCard } from "./IssueCard";
import { SuggestionModal } from "./SuggestionModal";
import { getSuggestionsBatch, generateCode, CleaningStep } from "@/lib/api";

interface AnalysisResultProps {
  analysis: {
    id?: string;
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
      encoding_detected?: string;
      delimiter_detected?: string;
    };
    raw_profile?: {
      memory_usage_mb?: number;
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
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const [selectedIssue, setSelectedIssue] = useState<any>(null);

  // Générer un échantillon réaliste pour le backend
  const generateRealisticSample = useCallback(() => {
    if (!originalData || originalData.length === 0) return null;

    const sampleSize = Math.min(100, originalData.length);
    const sample = originalData.slice(0, sampleSize);

    // Convertir en format adapté pour l'API
    const columns = Object.keys(sample[0]);
    const dataDict: Record<string, any[]> = {};

    columns.forEach((col) => {
      dataDict[col] = sample.map((row) => {
        const val = row[col];
        // Gérer les valeurs spéciales
        if (val === "" || val === " " || val === null || val === undefined) {
          return null;
        }
        if (typeof val === "string") {
          // Détecter si c'est une colonne numérique avec des strings
          const colProfile = analysis.columns[col];
          if (colProfile?.semantic_type?.includes("numeric")) {
            const num = parseFloat(val.replace(/,/g, ".").replace(/\s/g, ""));
            return isNaN(num) ? null : num;
          }
        }
        return val;
      });
    });

    return { columns, data: dataDict, size: sampleSize };
  }, [originalData, analysis.columns]);

  const toggleStep = (stepId: string) => {
    const newSteps = new Set(selectedSteps);
    if (newSteps.has(stepId)) {
      newSteps.delete(stepId);
    } else {
      newSteps.add(stepId);
    }
    setSelectedSteps(newSteps);
  };

  const selectAllCritical = () => {
    const critical = analysis.issues
      .filter((issue) => issue.severity === "high" || issue.severity === "critical")
      .map((issue) => `${issue.column}-${issue.issue}`);
    setSelectedSteps(new Set(critical));
  };

  const handleGetSuggestions = async (issue: any) => {
    setSelectedIssue(issue);
    setShowSuggestions(true);

    try {
      const sample = generateRealisticSample();

      const request = {
        dataset_name: analysis.dataset_info?.filename || "dataset.csv",
        column_types: Object.fromEntries(
          Object.entries(analysis.columns).map(([k, v]) => [k, v.dtype])
        ),
        issues: [{
          type: issue.issue || issue.type,
          column: issue.column,
          severity: issue.severity,
          description: issue.description,
          affected_rows: issue.affected_rows,
          count: issue.count,
          rate: issue.rate,
          semantic_type: issue.semantic_type || analysis.columns[issue.column]?.semantic_type
        }],
        sample_data: sample?.data
      };

      const response = await getSuggestionsBatch(request);
      setSuggestions(response.results[0]?.strategies || []);
    } catch (error) {
      console.error("Erreur suggestions:", error);
      // Fallback suggestions
      setSuggestions([{
        name: "Nettoyage automatique",
        description: "Stratégie par défaut",
        pros: ["Robuste", "Automatique"],
        cons: ["Peut nécessiter validation"],
        code_preview: `# Nettoyage pour ${issue.column}
df['${issue.column}'] = df['${issue.column}'].fillna('Unknown')`,
        confidence: "medium"
      }]);
    }
  };

  const handleGenerateCode = async () => {
    if (selectedSteps.size === 0) {
      alert("Veuillez sélectionner au moins un problème à résoudre");
      return;
    }

    setIsGenerating(true);
    setPreview(null);

    try {
      const sample = generateRealisticSample();

      // Préparer les étapes sélectionnées
      const selectedIssues = analysis.issues.filter((issue) =>
        selectedSteps.has(`${issue.column}-${issue.issue}`)
      );

      // Formater pour l'API avec code préliminaire
      const steps: CleaningStep[] = selectedIssues.map((issue) => ({
        column: issue.column,
        issue_type: issue.issue || issue.type || "unknown",
        strategy_name: issue.severity || "auto",
        code: `# ${issue.description || "Nettoyage"}
` +
              `if '${issue.column}' in df.columns:
` +
              `    # Conversion type si nécessaire
` +
              `    if df['${issue.column}'].dtype == 'object':
` +
              `        df['${issue.column}'] = pd.to_numeric(df['${issue.column}'], errors='coerce')
` +
              `    # Imputation
` +
              `    df['${issue.column}'] = df['${issue.column}'].fillna(df['${issue.column}'].median())
`
      }));

      const response = await generateCode({
        dataset_name: analysis.dataset_info?.filename || "dataset.csv",
        steps: steps,
      });

      if (response.script) {
        setPreview(response.script);
        onGenerateCode(response.script);
      }
    } catch (error) {
      console.error("Erreur génération:", error);
      alert("Erreur lors de la génération du code. Vérifiez la console.");
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

  return (
    <div className="space-y-6">
      {/* Résumé */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl p-6 border border-blue-100">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">📊 Résumé de l'analyse</h3>
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

        {analysis.dataset_info?.encoding_detected && (
          <div className="mt-4 flex gap-4 text-sm text-gray-600">
            <span>🔤 Encodage: <strong>{analysis.dataset_info.encoding_detected}</strong></span>
            <span>➗ Délimiteur: <strong>{analysis.dataset_info.delimiter_detected}</strong></span>
            {analysis.raw_profile?.memory_usage_mb && (
              <span>💾 Mémoire: <strong>{analysis.raw_profile.memory_usage_mb} MB</strong></span>
            )}
          </div>
        )}
      </div>

      {/* Liste des problèmes */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="p-4 bg-gray-50 border-b border-gray-200 flex justify-between items-center">
          <h3 className="font-semibold text-gray-800">🔍 Problèmes identifiés</h3>
          <button
            onClick={selectAllCritical}
            className="text-sm bg-blue-100 hover:bg-blue-200 text-blue-700 px-3 py-1 rounded-full transition-colors"
          >
            Sélectionner critiques
          </button>
        </div>

        <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
          {analysis.issues.map((issue, idx) => (
            <div
              key={`${issue.column}-${issue.issue}-${idx}`}
              className="p-4 hover:bg-gray-50 transition-colors flex items-start gap-3"
            >
              <input
                type="checkbox"
                checked={selectedSteps.has(`${issue.column}-${issue.issue}`)}
                onChange={() => toggleStep(`${issue.column}-${issue.issue}`)}
                className="mt-1 w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
              />
              <div className="flex-1" onClick={() => handleGetSuggestions(issue)}>
                <IssueCard
                  issue={issue}
                  index={idx}
                  onClick={() => handleGetSuggestions(issue)}
                  isSelected={selectedSteps.has(`${issue.column}-${issue.issue}`)}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Actions */}
      <div className="flex gap-3">
        <button
          onClick={handleGenerateCode}
          disabled={isGenerating || selectedSteps.size === 0}
          className="flex-1 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 
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
          <button
            onClick={downloadCode}
            className="bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-6 rounded-lg
                     transition-all shadow-lg"
          >
            💾 Télécharger
          </button>
        )}
      </div>

      {/* Aperçu du code */}
      {preview && (
        <div className="bg-gray-900 rounded-xl overflow-hidden shadow-2xl">
          <div className="bg-gray-800 px-4 py-3 flex justify-between items-center">
            <h3 className="text-gray-200 font-mono text-sm">📄 clean_dataset.py</h3>
            <span className="text-xs text-gray-400">Python 3</span>
          </div>
          <pre className="p-4 overflow-x-auto text-sm text-gray-300 font-mono leading-relaxed max-h-[500px] overflow-y-auto">
            {preview}
          </pre>
        </div>
      )}

      {/* Modal de suggestions */}
      {showSuggestions && selectedIssue && (
        <SuggestionModal
          issue={selectedIssue}
          strategies={suggestions}
          recommended={0}
          onSelect={(strategy) => {
            // Ajouter automatiquement l'étape sélectionnée
            toggleStep(`${selectedIssue.column}-${selectedIssue.issue}`);
            setShowSuggestions(false);
          }}
          onClose={() => setShowSuggestions(false)}
        />
      )}
    </div>
  );
};

export default AnalysisResult;
